/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.connector.mongodb.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.mongodb.testutils.MongoTestUtil;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.functions.table.lookup.LookupCacheManager;
import org.apache.flink.table.test.lookup.cache.LookupCacheAssert;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ChangeStreamPreAndPostImagesOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.COLLECTION;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.DATABASE;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.URI;
import static org.apache.flink.connector.mongodb.testutils.MongoTestUtil.MONGO_4_0;
import static org.apache.flink.connector.mongodb.testutils.MongoTestUtil.MONGO_5_0;
import static org.apache.flink.connector.mongodb.testutils.MongoTestUtil.MONGO_6_0;
import static org.apache.flink.connector.mongodb.testutils.MongoTestUtil.MONGO_IMAGE_PREFIX;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for lookup feature of {@link MongoDynamicTableSource}. */
@ExtendWith(ParameterizedTestExtension.class)
public class MongoLookupTableSourceITCase {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDynamicTableSinkITCase.class);

    private static final String TEST_DATABASE = "test";
    private static final String TEST_COLLECTION = "mongo_lookup_source";

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .build());

    @Parameter public String mongoVersion;

    @Parameter(value = 1)
    public Caching caching;

    private static final List<String> MONGO_VERSIONS =
            Arrays.asList(MONGO_4_0, MONGO_5_0, MONGO_6_0);

    @Parameters(name = "mongoVersion={0} caching={1}")
    public static List<Object[]> parameters() {
        List<Object[]> params = new ArrayList<>();
        for (String version : MONGO_VERSIONS) {
            params.add(new Object[] {version, Caching.DISABLE_CACHE});
            params.add(new Object[] {version, Caching.ENABLE_CACHE});
        }
        return params;
    }

    private MongoDBContainer mongoDBContainer;
    private MongoClient mongoClient;
    private MongoDatabase testDatabase;
    private MongoCollection<BsonDocument> testCollection;

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;

    @BeforeEach
    void before() {
        mongoDBContainer =
                MongoTestUtil.createMongoDBContainer(MONGO_IMAGE_PREFIX + mongoVersion, LOG);
        mongoDBContainer.start();
        mongoClient = MongoClients.create(mongoDBContainer.getConnectionString());
        testDatabase = mongoClient.getDatabase(TEST_DATABASE);
        testCollection =
                testDatabase.getCollection(TEST_COLLECTION).withDocumentClass(BsonDocument.class);

        if (MONGO_6_0.equals(mongoVersion)) {
            testDatabase.createCollection(
                    TEST_COLLECTION,
                    new CreateCollectionOptions()
                            .changeStreamPreAndPostImagesOptions(
                                    new ChangeStreamPreAndPostImagesOptions(true)));
        }

        List<BsonDocument> testRecords = Arrays.asList(createTestData(1), createTestData(2));
        testCollection.insertMany(testRecords);

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
    }

    @AfterEach
    void after() {
        if (mongoClient != null) {
            mongoClient.close();
        }
        mongoDBContainer.close();
    }

    @TestTemplate
    public void testLookupJoin() throws Exception {
        // Create MongoDB lookup table
        Map<String, String> lookupOptions = new HashMap<>();
        if (caching.equals(Caching.ENABLE_CACHE)) {
            lookupOptions.put(LookupOptions.CACHE_TYPE.key(), "PARTIAL");
            lookupOptions.put(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE.key(), "10min");
            lookupOptions.put(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS.key(), "10min");
            lookupOptions.put(LookupOptions.PARTIAL_CACHE_MAX_ROWS.key(), "100");
            lookupOptions.put(LookupOptions.MAX_RETRIES.key(), "10");
        }

        tEnv.executeSql(createTestDDl(lookupOptions));

        DataStream<Row> sourceStream =
                env.fromCollection(
                                Arrays.asList(
                                        Row.of(1L, "Alice"),
                                        Row.of(1L, "Alice"),
                                        Row.of(2L, "Bob"),
                                        Row.of(3L, "Charlie")))
                        .returns(
                                new RowTypeInfo(
                                        new TypeInformation[] {Types.LONG, Types.STRING},
                                        new String[] {"id", "name"}));

        Schema sourceSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("name", DataTypes.STRING())
                        .columnByExpression("proctime", "PROCTIME()")
                        .build();

        tEnv.createTemporaryView("value_source", sourceStream, sourceSchema);

        if (caching == Caching.ENABLE_CACHE) {
            LookupCacheManager.keepCacheOnRelease(true);
        }

        // Execute lookup join
        try (CloseableIterator<Row> iterator =
                tEnv.executeSql(
                                "SELECT S.id, S.name, D._id, D.f1, D.f2 FROM value_source"
                                        + " AS S JOIN mongo_source for system_time as of S.proctime AS D ON S.id = D._id")
                        .collect()) {
            List<String> result =
                    CollectionUtil.iteratorToList(iterator).stream()
                            .map(Row::toString)
                            .sorted()
                            .collect(Collectors.toList());
            List<String> expected =
                    Arrays.asList(
                            "+I[1, Alice, 1, 2, false]",
                            "+I[1, Alice, 1, 2, false]",
                            "+I[2, Bob, 2, 2, false]");

            assertThat(result).hasSize(3);
            assertThat(result).isEqualTo(expected);
            if (caching == Caching.ENABLE_CACHE) {
                // Validate cache
                Map<String, LookupCacheManager.RefCountedCache> managedCaches =
                        LookupCacheManager.getInstance().getManagedCaches();
                assertThat(managedCaches).hasSize(1);
                LookupCache cache =
                        managedCaches.get(managedCaches.keySet().iterator().next()).getCache();
                validateCachedValues(cache);
            }

        } finally {
            if (caching == Caching.ENABLE_CACHE) {
                LookupCacheManager.getInstance().checkAllReleased();
                LookupCacheManager.getInstance().clear();
                LookupCacheManager.keepCacheOnRelease(false);
            }
        }
    }

    private static void validateCachedValues(LookupCache cache) {
        // mongo does support project push down, the cached row has been projected
        RowData key1 = GenericRowData.of(1L);
        RowData value1 = GenericRowData.of(1L, StringData.fromString("2"), false);

        RowData key2 = GenericRowData.of(2L);
        RowData value2 = GenericRowData.of(2L, StringData.fromString("2"), false);

        RowData key3 = GenericRowData.of(3L);

        Map<RowData, Collection<RowData>> expectedEntries = new HashMap<>();
        expectedEntries.put(key1, Collections.singletonList(value1));
        expectedEntries.put(key2, Collections.singletonList(value2));
        expectedEntries.put(key3, Collections.emptyList());

        LookupCacheAssert.assertThat(cache).containsExactlyEntriesOf(expectedEntries);
    }

    private enum Caching {
        ENABLE_CACHE,
        DISABLE_CACHE
    }

    private String createTestDDl(Map<String, String> extraOptions) {
        Map<String, String> options = new HashMap<>();
        options.put(CONNECTOR.key(), "mongodb");
        options.put(URI.key(), mongoDBContainer.getConnectionString());
        options.put(DATABASE.key(), TEST_DATABASE);
        options.put(COLLECTION.key(), TEST_COLLECTION);
        if (extraOptions != null) {
            options.putAll(extraOptions);
        }

        String optionString =
                options.entrySet().stream()
                        .map(e -> String.format("'%s' = '%s'", e.getKey(), e.getValue()))
                        .collect(Collectors.joining(",\n"));

        return String.join(
                "\n",
                Arrays.asList(
                        "CREATE TABLE mongo_source",
                        "(",
                        "  _id BIGINT PRIMARY KEY NOT ENFORCED,",
                        "  f1 STRING,",
                        "  f2 BOOLEAN,",
                        "  f3 BINARY",
                        ") WITH (",
                        optionString,
                        ")"));
    }

    private static BsonDocument createTestData(long id) {
        return new BsonDocument()
                .append("_id", new BsonInt64(id))
                .append("f1", new BsonString("2"))
                .append("f2", BsonBoolean.FALSE)
                .append("f3", new BsonBinary(new byte[] {(byte) 3}));
    }
}
