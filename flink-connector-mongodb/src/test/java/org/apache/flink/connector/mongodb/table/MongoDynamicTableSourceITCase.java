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
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
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
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link MongoDynamicTableSource}. */
@Testcontainers
class MongoDynamicTableSourceITCase {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDynamicTableSinkITCase.class);

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .build());

    @Container
    private static final MongoDBContainer MONGO_CONTAINER =
            MongoTestUtil.createMongoDBContainer(LOG);

    private static final String TEST_DATABASE = "test";
    private static final String TEST_COLLECTION = "mongo_table_source";

    private static MongoClient mongoClient;

    private static StreamExecutionEnvironment env;
    private static StreamTableEnvironment tEnv;

    @BeforeAll
    static void beforeAll() {
        mongoClient = MongoClients.create(MONGO_CONTAINER.getConnectionString());

        MongoCollection<BsonDocument> coll =
                mongoClient
                        .getDatabase(TEST_DATABASE)
                        .getCollection(TEST_COLLECTION)
                        .withDocumentClass(BsonDocument.class);

        coll.insertMany(createTestData());
    }

    @AfterAll
    static void afterAll() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @BeforeEach
    void before() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    }

    @Test
    void testSource() {
        tEnv.executeSql(createTestDDl(null));

        List<Row> result = executeQueryToList("SELECT * FROM mongo_source");

        assertThat(result).isEqualTo(expectedRows());
    }

    @Test
    void testProject() {
        tEnv.executeSql(createTestDDl(null));

        List<Row> result = executeQueryToList("SELECT f1, f10 FROM mongo_source");

        List<Row> expected = Arrays.asList(Row.of("2", Row.of(13)), Row.of("3", Row.of(14)));

        assertThat(result).isEqualTo(expected);
    }

    @Test
    void testLimit() {
        tEnv.executeSql(createTestDDl(null));

        List<Row> result = executeQueryToList("SELECT * FROM mongo_source LIMIT 1");

        assertThat(result).hasSize(1);
        assertThat(result).containsAnyElementsOf(expectedRows());
    }

    @ParameterizedTest
    @EnumSource(Caching.class)
    void testLookupJoin(Caching caching) throws Exception {
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
                executeQuery(
                        "SELECT S.id, S.name, D._id, D.f1, D.f2 FROM value_source"
                                + " AS S JOIN mongo_source for system_time as of S.proctime AS D ON S.id = D._id")) {
            List<Row> result = CollectionUtil.iteratorToList(iterator);

            List<Row> expected =
                    Arrays.asList(
                            Row.of(1L, "Alice", 1L, "2", true),
                            Row.of(1L, "Alice", 1L, "2", true),
                            Row.of(2L, "Bob", 2L, "3", false));

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

    @Test
    void testFilter() {
        tEnv.executeSql(createTestDDl(null));

        // we create a VIEW here to test column remapping, i.e. would filter push down work if we
        // create a view that depends on our source table
        tEnv.executeSql(
                "CREATE VIEW fake_table (idx, f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12)"
                        + " as (SELECT * from mongo_source )");

        List<Row> allRows = executeQueryToList("SELECT * FROM mongo_source");
        assertThat(allRows).hasSize(2);

        Row onlyRow1 =
                allRows.stream()
                        .filter(row -> row.getFieldAs(0).equals(1L))
                        .findAny()
                        .orElseThrow(NullPointerException::new);

        Row onlyRow2 =
                allRows.stream()
                        .filter(row -> row.getFieldAs(0).equals(2L))
                        .findAny()
                        .orElseThrow(NullPointerException::new);

        // test the EQUALS filter
        assertThat(executeQueryToList("SELECT * FROM fake_table WHERE 1 = idx"))
                .containsExactly(onlyRow1);

        // test TIMESTAMP filter
        assertThat(
                        executeQueryToList(
                                "SELECT * FROM fake_table WHERE f5 = TIMESTAMP '2022-09-07 10:25:28.127'"))
                .containsExactly(onlyRow1);

        // test the IN operator
        assertThat(executeQueryToList("SELECT * FROM fake_table WHERE idx IN (2, 3)"))
                .containsExactly(onlyRow2);

        // test the NOT IN operator
        assertThat(
                        executeQueryToList(
                                "SELECT * FROM fake_table WHERE f7 NOT IN (CAST(1.0 AS DOUBLE), CAST(1.1 AS DOUBLE))"))
                .containsExactly(onlyRow1);

        // test mixing AND and OR operator
        assertThat(executeQueryToList("SELECT * FROM fake_table WHERE idx <> 1 OR f8 = 1.10"))
                .containsExactlyInAnyOrderElementsOf(allRows);

        // test mixing AND/OR with parenthesis, and the swapping the operand of equal expression
        assertThat(
                        executeQueryToList(
                                "SELECT * FROM fake_table WHERE (f0 IS NOT NULL AND f2 IS TRUE) OR f8 = 102.2"))
                .containsExactly(onlyRow1);

        // test Greater than and Less than
        assertThat(executeQueryToList("SELECT * FROM fake_table WHERE f8 > 1.09 AND f8 < 1.11"))
                .containsExactly(onlyRow1);

        // One more test of parenthesis
        assertThat(
                        executeQueryToList(
                                "SELECT * FROM fake_table WHERE f0 IS NULL AND (f8 >= 1.11 OR f4 <= 5)"))
                .containsExactly(onlyRow2);

        assertThat(
                        executeQueryToList(
                                "SELECT * FROM mongo_source WHERE _id = 2 AND f7 > 0.8 OR f7 < 1.1"))
                .containsExactlyInAnyOrderElementsOf(allRows);

        assertThat(
                        executeQueryToList(
                                "SELECT * FROM mongo_source WHERE 1 = _id AND f1 NOT IN ('2', '3')"))
                .isEmpty();
    }

    private static void validateCachedValues(LookupCache cache) {
        // mongo does support project push down, the cached row has been projected
        RowData key1 = GenericRowData.of(1L);
        RowData value1 = GenericRowData.of(1L, StringData.fromString("2"), true);

        RowData key2 = GenericRowData.of(2L);
        RowData value2 = GenericRowData.of(2L, StringData.fromString("3"), false);

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

    private static String createTestDDl(Map<String, String> extraOptions) {
        Map<String, String> options = new HashMap<>();
        options.put(CONNECTOR.key(), "mongodb");
        options.put(URI.key(), MONGO_CONTAINER.getConnectionString());
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
                        "  _id BIGINT,",
                        "  f0 STRING,",
                        "  f1 STRING,",
                        "  f2 BOOLEAN,",
                        "  f3 BINARY,",
                        "  f4 INTEGER,",
                        "  f5 TIMESTAMP_LTZ(3),",
                        "  f6 TIMESTAMP_LTZ(0),",
                        "  f7 DOUBLE,",
                        "  f8 DECIMAL(10, 2),",
                        "  f9 MAP<STRING, INTEGER>,",
                        "  f10 ROW<k INTEGER>,",
                        "  f11 ARRAY<STRING>,",
                        "  f12 ARRAY<ROW<k STRING>>",
                        ") WITH (",
                        optionString,
                        ")"));
    }

    private static List<Row> expectedRows() {
        return Arrays.asList(
                Row.of(
                        1L,
                        "",
                        "2",
                        true,
                        new byte[] {(byte) 3},
                        6,
                        Instant.ofEpochMilli(1662546328127L),
                        Instant.ofEpochSecond(1662546328L),
                        0.9d,
                        new BigDecimal("1.10"),
                        Collections.singletonMap("k", 12),
                        Row.of(13),
                        new String[] {"11_1", "11_2"},
                        new Row[] {Row.of("12_1"), Row.of("12_2")}),
                Row.of(
                        2L,
                        null,
                        "3",
                        false,
                        new byte[] {(byte) 4},
                        7,
                        Instant.ofEpochMilli(1662546328128L),
                        Instant.ofEpochSecond(1662546329L),
                        1.0d,
                        new BigDecimal("1.11"),
                        Collections.singletonMap("k", 13),
                        Row.of(14),
                        new String[] {"11_3", "11_4"},
                        new Row[] {Row.of("12_3"), Row.of("12_4")}));
    }

    private static List<BsonDocument> createTestData() {
        return Arrays.asList(
                new BsonDocument()
                        .append("_id", new BsonInt64(1L))
                        .append("f0", new BsonString(""))
                        .append("f1", new BsonString("2"))
                        .append("f2", BsonBoolean.TRUE)
                        .append("f3", new BsonBinary(new byte[] {(byte) 3}))
                        .append("f4", new BsonInt32(6))
                        // 2022-09-07T10:25:28.127Z
                        .append("f5", new BsonDateTime(1662546328127L))
                        .append("f6", new BsonTimestamp(1662546328, 0))
                        .append("f7", new BsonDouble(0.9d))
                        .append("f8", new BsonDecimal128(new Decimal128(new BigDecimal("1.10"))))
                        .append("f9", new BsonDocument("k", new BsonInt32(12)))
                        .append("f10", new BsonDocument("k", new BsonInt32(13)))
                        .append(
                                "f11",
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("11_1"), new BsonString("11_2"))))
                        .append(
                                "f12",
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonDocument("k", new BsonString("12_1")),
                                                new BsonDocument("k", new BsonString("12_2"))))),
                new BsonDocument()
                        .append("_id", new BsonInt64(2L))
                        .append("f0", BsonNull.VALUE)
                        .append("f1", new BsonString("3"))
                        .append("f2", BsonBoolean.FALSE)
                        .append("f3", new BsonBinary(new byte[] {(byte) 4}))
                        .append("f4", new BsonInt32(7))
                        // 2022-09-07T10:25:28.128Z
                        .append("f5", new BsonDateTime(1662546328128L))
                        .append("f6", new BsonTimestamp(1662546329, 0))
                        .append("f7", new BsonDouble(1.0d))
                        .append("f8", new BsonDecimal128(new Decimal128(new BigDecimal("1.11"))))
                        .append("f9", new BsonDocument("k", new BsonInt32(13)))
                        .append("f10", new BsonDocument("k", new BsonInt32(14)))
                        .append(
                                "f11",
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("11_3"), new BsonString("11_4"))))
                        .append(
                                "f12",
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonDocument("k", new BsonString("12_3")),
                                                new BsonDocument("k", new BsonString("12_4"))))));
    }

    private static List<Row> executeQueryToList(String sql) {
        return CollectionUtil.iteratorToList(executeQuery(sql));
    }

    private static CloseableIterator<Row> executeQuery(String sql) {
        return tEnv.executeSql(sql).collect();
    }
}
