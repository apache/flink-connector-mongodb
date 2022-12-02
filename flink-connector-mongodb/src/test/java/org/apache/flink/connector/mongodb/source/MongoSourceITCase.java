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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoJsonDeserializationSchema;
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
import org.apache.flink.util.CollectionUtil;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.lang3.RandomStringUtils;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_FIELD;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for using Mongo Sink. */
@Testcontainers
public class MongoSourceITCase {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .build());

    @RegisterExtension
    static final MongoShardedContainers MONGO_SHARDED_CONTAINER =
            MongoTestUtil.createMongoDBShardedContainers(Network.newNetwork());

    private static MongoClient mongoClient;

    private static final String ADMIN_DATABASE = "admin";
    private static final String CONFIG_DATABASE = "config";
    private static final String SETTINGS_COLLECTION = "settings";
    private static final String CHUNK_SIZE_FIELD = "chunksize";
    private static final String VALUE_FIELD = "value";

    private static final String TEST_DATABASE = "test_source";
    private static final String TEST_COLLECTION = "test_coll";
    private static final String TEST_SHARDED_COLLECTION = "test_sharded_coll";

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
        UpdateResult result =
                settings.updateOne(
                        Filters.eq(ID_FIELD, CHUNK_SIZE_FIELD),
                        Updates.combine(
                                Updates.set(ID_FIELD, CHUNK_SIZE_FIELD),
                                Updates.set(VALUE_FIELD, 1)),
                        new UpdateOptions().upsert(true));

        // make test data for non-sharded collection.
        initTestData(TEST_COLLECTION);
        // make test data for sharded collection.
        initTestData(TEST_SHARDED_COLLECTION);

        MongoDatabase admin = mongoClient.getDatabase(ADMIN_DATABASE);
        // shard test collection with sharded key _id
        admin.runCommand(
                BsonDocument.parse(String.format("{ enableSharding: '%s'}", TEST_DATABASE)));
        admin.runCommand(
                BsonDocument.parse(
                        String.format(
                                "{ shardCollection : '%s.%s', key : {_id : 1} }",
                                TEST_DATABASE, TEST_SHARDED_COLLECTION)));
    }

    @AfterAll
    static void afterAll() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @ParameterizedTest
    @EnumSource(PartitionStrategy.class)
    public void testPartitionStrategy(PartitionStrategy partitionStrategy) throws Exception {
        String collection =
                partitionStrategy == PartitionStrategy.SHARDED
                        ? TEST_SHARDED_COLLECTION
                        : TEST_COLLECTION;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        MongoSource<RowData> mongoSource =
                defaultSourceBuilder(collection)
                        .setPartitionSize(MemorySize.parse("1mb"))
                        .setSamplesPerPartition(3)
                        .setPartitionStrategy(partitionStrategy)
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
    public void testLimit() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final int limitSize = 100;
        MongoSource<RowData> mongoSource =
                defaultSourceBuilder(TEST_COLLECTION).setLimit(limitSize).build();

        List<RowData> results =
                CollectionUtil.iteratorToList(
                        env.fromSource(
                                        mongoSource,
                                        WatermarkStrategy.noWatermarks(),
                                        "MongoDB-Source")
                                .executeAndCollect());

        assertThat(results).hasSize(limitSize);
    }

    @Test
    public void testProject() throws Exception {
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
                Column.physical("f0", DataTypes.INT()), Column.physical("f1", DataTypes.STRING()));
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
                .append("f1", new BsonString(RandomStringUtils.randomAlphabetic(32)));
    }
}
