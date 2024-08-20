/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.mongodb.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.connector.mongodb.table.serialization.MongoRowDataDeserializationSchema;
import org.apache.flink.connector.mongodb.testutils.MongoShardedContainers;
import org.apache.flink.connector.mongodb.testutils.MongoTestUtil;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.CollectionUtil;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.TimeSeriesOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import org.apache.commons.lang3.RandomStringUtils;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.DEFAULT_JSON_WRITER_SETTINGS;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_FIELD;
import static org.apache.flink.connector.mongodb.testutils.MongoTestUtil.CHUNK_SIZE_FIELD;
import static org.apache.flink.connector.mongodb.testutils.MongoTestUtil.CONFIG_DATABASE;
import static org.apache.flink.connector.mongodb.testutils.MongoTestUtil.SETTINGS_COLLECTION;
import static org.apache.flink.connector.mongodb.testutils.MongoTestUtil.VALUE_FIELD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/** IT cases for using Mongo Source. */
@Testcontainers
class MongoSourceITCase {

    private static final int PARALLELISM = 2;

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(PARALLELISM)
                            .build());

    @RegisterExtension
    private static final MongoShardedContainers MONGO_SHARDED_CONTAINER =
            MongoTestUtil.createMongoDBShardedContainers(Network.newNetwork());

    @RegisterExtension
    private final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

    private static MongoClient mongoClient;

    private static final String TEST_DATABASE = "test_source";
    private static final String TEST_COLLECTION = "test_coll";
    private static final String TEST_TIMESERIES_COLLECTION = "test_timeseries_collection";
    private static final String TEST_SHARDED_COLLECTION = "test_sharded_coll";
    private static final String TEST_HASHED_KEY_SHARDED_COLLECTION = "test_hashed_key_sharded_coll";

    private static final int TEST_RECORD_SIZE = 30000;
    private static final int TEST_RECORD_BATCH_SIZE = 10000;

    @BeforeAll
    static void beforeAll() {
        mongoClient = MongoClients.create(MONGO_SHARDED_CONTAINER.getConnectionString());
        MongoCollection<BsonDocument> settings =
                mongoClient
                        .getDatabase(CONFIG_DATABASE)
                        .getCollection(SETTINGS_COLLECTION)
                        .withDocumentClass(BsonDocument.class);
        // decrease chunk size to 1mb to make splitter test easier.
        settings.updateOne(
                Filters.eq(ID_FIELD, CHUNK_SIZE_FIELD),
                Updates.combine(
                        Updates.set(ID_FIELD, CHUNK_SIZE_FIELD), Updates.set(VALUE_FIELD, 1)),
                new UpdateOptions().upsert(true));

        // make test data for non-sharded collection.
        initTestData(TEST_COLLECTION);
        // make test data for sharded collection.
        initTestData(TEST_SHARDED_COLLECTION);
        // make test data for hashed key sharded collection.
        initTestData(TEST_HASHED_KEY_SHARDED_COLLECTION);

        // create unique index {f0: 1, f1: 1}.
        Bson indexKeys = BsonDocument.parse("{ f0: 1, f1: 1 }");
        MongoTestUtil.createIndex(
                mongoClient,
                TEST_DATABASE,
                TEST_SHARDED_COLLECTION,
                indexKeys,
                new IndexOptions().unique(true));
        MongoTestUtil.shardCollection(
                mongoClient, TEST_DATABASE, TEST_SHARDED_COLLECTION, indexKeys);

        // create hashed index {f1: 'hashed'}.
        Bson hashedIndexKeys = BsonDocument.parse("{ f1: 'hashed' }");
        MongoTestUtil.createIndex(
                mongoClient,
                TEST_DATABASE,
                TEST_HASHED_KEY_SHARDED_COLLECTION,
                hashedIndexKeys,
                new IndexOptions());
        MongoTestUtil.shardCollection(
                mongoClient, TEST_DATABASE, TEST_HASHED_KEY_SHARDED_COLLECTION, hashedIndexKeys);
    }

    @AfterAll
    static void afterAll() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @ParameterizedTest
    @MethodSource("providePartitionStrategyAndCollection")
    void testPartitionStrategy(PartitionStrategy partitionStrategy, String collection)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        MongoSource<RowData> mongoSource =
                defaultSourceBuilder(collection)
                        .setPartitionSize(MemorySize.parse("1mb"))
                        .setSamplesPerPartition(3)
                        .setPartitionStrategy(partitionStrategy)
                        .setFilter(Filters.gt("f0", new BsonInt32(10000)))
                        .build();

        List<RowData> results =
                CollectionUtil.iteratorToList(
                        env.fromSource(
                                        mongoSource,
                                        WatermarkStrategy.noWatermarks(),
                                        "MongoDB-Source")
                                .executeAndCollect());

        assertThat(results).hasSize(TEST_RECORD_SIZE - 10000);
    }

    @Test
    void testPartitionStrategyOnTimeSeriesCollection() throws Exception {
        assumeThat(MongoTestUtil.mongoVersion().isAtLeast(5, 0, 0))
                .as("Time series collection is only supported in MongoDB 5.0.0 or later.")
                .isTrue();

        mongoClient
                .getDatabase(TEST_DATABASE)
                .createCollection(
                        TEST_TIMESERIES_COLLECTION,
                        new CreateCollectionOptions()
                                .timeSeriesOptions(new TimeSeriesOptions("f2"))
                                .expireAfter(1, TimeUnit.HOURS));
        initTestData(TEST_TIMESERIES_COLLECTION);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        MongoSource<RowData> mongoSource =
                defaultSourceBuilder(TEST_TIMESERIES_COLLECTION)
                        .setPartitionSize(MemorySize.parse("1mb"))
                        .setSamplesPerPartition(3)
                        .setPartitionStrategy(PartitionStrategy.SAMPLE)
                        .build();

        List<RowData> results =
                CollectionUtil.iteratorToList(
                        env.fromSource(
                                        mongoSource,
                                        WatermarkStrategy.noWatermarks(),
                                        "MongoDB-Source")
                                .executeAndCollect());

        assertThat(results).hasSize(TEST_RECORD_SIZE);
    }

    @Test
    void testLimit() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final int limitSize = 100;
        MongoSource<RowData> mongoSource =
                defaultSourceBuilder(TEST_COLLECTION)
                        .setLimit(limitSize)
                        .setPartitionSize(MemorySize.parse("1mb"))
                        .build();

        List<RowData> results =
                CollectionUtil.iteratorToList(
                        env.fromSource(
                                        mongoSource,
                                        WatermarkStrategy.noWatermarks(),
                                        "MongoDB-Source")
                                .executeAndCollect());

        assertThat(results).hasSize(limitSize * PARALLELISM);
    }

    @Test
    void testProject() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        MongoSource<String> mongoSource =
                MongoSource.<String>builder()
                        .setUri(MONGO_SHARDED_CONTAINER.getConnectionString())
                        .setDatabase(TEST_DATABASE)
                        .setCollection(TEST_COLLECTION)
                        .setProjectedFields("f0")
                        .setDeserializationSchema(new MongoJsonDeserializationSchema())
                        .build();

        List<String> results =
                CollectionUtil.iteratorToList(
                        env.fromSource(
                                        mongoSource,
                                        WatermarkStrategy.noWatermarks(),
                                        "MongoDB-Source")
                                .executeAndCollect());

        assertThat(results).hasSize(TEST_RECORD_SIZE);
        assertThat(Document.parse(results.get(0))).containsOnlyKeys("f0");
    }

    @ParameterizedTest
    @MethodSource("providePartitionStrategyAndCollection")
    void testRecovery(PartitionStrategy partitionStrategy, String collection) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(200L);

        MongoSource<RowData> mongoSource =
                defaultSourceBuilder(collection)
                        .setPartitionStrategy(partitionStrategy)
                        .setPartitionSize(MemorySize.parse("6mb"))
                        .setProjectedFields("f0")
                        .setFetchSize(100)
                        .build();

        final SharedReference<AtomicBoolean> failed = sharedObjects.add(new AtomicBoolean(false));

        List<RowData> results =
                CollectionUtil.iteratorToList(
                        env.fromSource(
                                        mongoSource,
                                        WatermarkStrategy.noWatermarks(),
                                        "MongoDB-Source")
                                .map(new FailingMapper(failed))
                                .executeAndCollect());

        assertThat(results).hasSize(TEST_RECORD_SIZE);
    }

    private static Stream<Arguments> providePartitionStrategyAndCollection() {
        return Stream.of(
                Arguments.of(PartitionStrategy.SINGLE, TEST_COLLECTION),
                Arguments.of(PartitionStrategy.SPLIT_VECTOR, TEST_COLLECTION),
                Arguments.of(PartitionStrategy.SAMPLE, TEST_COLLECTION),
                Arguments.of(PartitionStrategy.SHARDED, TEST_SHARDED_COLLECTION),
                Arguments.of(PartitionStrategy.SHARDED, TEST_HASHED_KEY_SHARDED_COLLECTION));
    }

    private static MongoSourceBuilder<RowData> defaultSourceBuilder(String collection) {
        ResolvedSchema schema = defaultSourceSchema();
        RowType rowType = (RowType) schema.toPhysicalRowDataType().getLogicalType();
        TypeInformation<RowData> typeInfo = InternalTypeInfo.of(rowType);

        return MongoSource.<RowData>builder()
                .setUri(MONGO_SHARDED_CONTAINER.getConnectionString())
                .setDatabase(TEST_DATABASE)
                .setCollection(collection)
                .setDeserializationSchema(new MongoRowDataDeserializationSchema(rowType, typeInfo));
    }

    private static ResolvedSchema defaultSourceSchema() {
        return ResolvedSchema.of(
                Column.physical("f0", DataTypes.INT()),
                Column.physical("f1", DataTypes.STRING()),
                Column.physical("f2", DataTypes.TIMESTAMP_LTZ(3)));
    }

    private static void initTestData(String collection) {
        MongoCollection<BsonDocument> coll =
                mongoClient
                        .getDatabase(TEST_DATABASE)
                        .getCollection(collection)
                        .withDocumentClass(BsonDocument.class);

        List<BsonDocument> testRecords = new ArrayList<>();
        for (int i = 1; i <= TEST_RECORD_SIZE; i++) {
            testRecords.add(createTestData(i));
            if (testRecords.size() >= TEST_RECORD_BATCH_SIZE) {
                coll.insertMany(testRecords);
                testRecords.clear();
            }
        }
    }

    private static BsonDocument createTestData(int id) {
        return new BsonDocument("f0", new BsonInt32(id))
                .append("f1", new BsonString(RandomStringUtils.randomAlphabetic(32)))
                .append("f2", new BsonDateTime(System.currentTimeMillis()));
    }

    private static class MongoJsonDeserializationSchema
            implements MongoDeserializationSchema<String> {

        @Override
        public String deserialize(BsonDocument document) {
            return Optional.ofNullable(document)
                    .map(doc -> doc.toJson(DEFAULT_JSON_WRITER_SETTINGS))
                    .orElse(null);
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }

    private static class FailingMapper
            implements MapFunction<RowData, RowData>, CheckpointListener {

        private final SharedReference<AtomicBoolean> failed;
        private int emittedRecords = 0;

        private FailingMapper(SharedReference<AtomicBoolean> failed) {
            this.failed = failed;
        }

        @Override
        public RowData map(RowData value) {
            emittedRecords++;
            return value;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            if (failed.get().get() || emittedRecords == 0) {
                return;
            }
            failed.get().set(true);
            throw new Exception("Expected failure");
        }
    }
}
