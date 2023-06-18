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

import org.apache.flink.connector.mongodb.table.config.FullDocumentStrategy;
import org.apache.flink.connector.mongodb.testutils.MongoTestUtil;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
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
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.CHANGE_STREAM_FULL_DOCUMENT_STRATEGY;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.COLLECTION;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.DATABASE;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.URI;
import static org.apache.flink.connector.mongodb.testutils.MongoTestUtil.MONGO_4_0;
import static org.apache.flink.connector.mongodb.testutils.MongoTestUtil.MONGO_5_0;
import static org.apache.flink.connector.mongodb.testutils.MongoTestUtil.MONGO_6_0;
import static org.apache.flink.connector.mongodb.testutils.MongoTestUtil.MONGO_IMAGE_PREFIX;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link MongoDynamicTableSource}. */
@ExtendWith(ParameterizedTestExtension.class)
public class MongoDynamicTableSourceITCase {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDynamicTableSinkITCase.class);

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .build());

    @Parameter public String mongoVersion;

    @Parameters(name = "mongoVersion={0}")
    public static List<String> parameters() {
        return Arrays.asList(MONGO_4_0, MONGO_5_0, MONGO_6_0);
    }

    private static final String TEST_DATABASE = "test";
    private static final String TEST_COLLECTION = "mongo_table_source";

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
    public void testSource() {
        tEnv.executeSql(createTestDDl(null));

        Iterator<Row> collected = tEnv.executeSql("SELECT * FROM mongo_source").collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        List<String> expected =
                Stream.of(
                                "+I[1, 2, false, [3], 6, 2022-09-07T10:25:28.127Z, 2022-09-07T10:25:28Z, 0.9, 1.10, {k=12}, +I[13], [11_1, 11_2], [+I[12_1], +I[12_2]]]",
                                "+I[2, 2, false, [3], 6, 2022-09-07T10:25:28.127Z, 2022-09-07T10:25:28Z, 0.9, 1.10, {k=12}, +I[13], [11_1, 11_2], [+I[12_1], +I[12_2]]]")
                        .sorted()
                        .collect(Collectors.toList());

        assertThat(result).isEqualTo(expected);
    }

    @TestTemplate
    public void testUnboundedSource() throws Exception {
        // Create MongoDB lookup table
        Map<String, String> options = new HashMap<>();
        options.put(
                SCAN_STARTUP_MODE.key(), MongoConnectorOptions.ScanStartupMode.INITIAL.toString());
        if (MONGO_6_0.equals(mongoVersion)) {
            options.put(
                    CHANGE_STREAM_FULL_DOCUMENT_STRATEGY.key(),
                    FullDocumentStrategy.PRE_AND_POST_IMAGES.toString());
        }

        tEnv.executeSql(createTestDDl(options));
        TableResult tableResult = tEnv.executeSql("SELECT * FROM mongo_source");

        try (CloseableIterator<Row> iterator = tableResult.collect()) {
            // fetch scan records
            List<String> scanned = fetchRows(iterator, 2);
            List<String> expected =
                    Arrays.asList(
                            "+I[1, 2, false, [3], 6, 2022-09-07T10:25:28.127Z, 2022-09-07T10:25:28Z, 0.9, 1.10, {k=12}, +I[13], [11_1, 11_2], [+I[12_1], +I[12_2]]]",
                            "+I[2, 2, false, [3], 6, 2022-09-07T10:25:28.127Z, 2022-09-07T10:25:28Z, 0.9, 1.10, {k=12}, +I[13], [11_1, 11_2], [+I[12_1], +I[12_2]]]");
            // assert scanned records
            assertThat(scanned).containsExactlyElementsOf(expected);

            // insert 2 records
            List<BsonDocument> newDocs =
                    Arrays.asList(createTestData(3), createTestData(4), createTestData(5));
            testCollection.insertMany(newDocs);
            List<String> inserted = fetchRows(iterator, 3);
            expected =
                    Arrays.asList(
                            "+I[3, 2, false, [3], 6, 2022-09-07T10:25:28.127Z, 2022-09-07T10:25:28Z, 0.9, 1.10, {k=12}, +I[13], [11_1, 11_2], [+I[12_1], +I[12_2]]]",
                            "+I[4, 2, false, [3], 6, 2022-09-07T10:25:28.127Z, 2022-09-07T10:25:28Z, 0.9, 1.10, {k=12}, +I[13], [11_1, 11_2], [+I[12_1], +I[12_2]]]",
                            "+I[5, 2, false, [3], 6, 2022-09-07T10:25:28.127Z, 2022-09-07T10:25:28Z, 0.9, 1.10, {k=12}, +I[13], [11_1, 11_2], [+I[12_1], +I[12_2]]]");
            // assert inserted records
            assertThat(inserted).containsExactlyElementsOf(expected);

            // delete 1 record
            testCollection.deleteOne(Filters.eq("_id", newDocs.get(0).getInt64("_id")));
            List<String> deleted = fetchRows(iterator, 1);
            expected =
                    Collections.singletonList(
                            "-D[3, 2, false, [3], 6, 2022-09-07T10:25:28.127Z, 2022-09-07T10:25:28Z, 0.9, 1.10, {k=12}, +I[13], [11_1, 11_2], [+I[12_1], +I[12_2]]]");
            // assert deleted records
            assertThat(deleted).containsExactlyElementsOf(expected);

            // update 1 record
            testCollection.updateOne(
                    Filters.eq("_id", newDocs.get(1).getInt64("_id")),
                    Updates.set("f1", new BsonString("3")));
            List<String> updated = fetchRows(iterator, 2);
            expected =
                    Arrays.asList(
                            "-U[4, 2, false, [3], 6, 2022-09-07T10:25:28.127Z, 2022-09-07T10:25:28Z, 0.9, 1.10, {k=12}, +I[13], [11_1, 11_2], [+I[12_1], +I[12_2]]]",
                            "+U[4, 3, false, [3], 6, 2022-09-07T10:25:28.127Z, 2022-09-07T10:25:28Z, 0.9, 1.10, {k=12}, +I[13], [11_1, 11_2], [+I[12_1], +I[12_2]]]");
            // assert updated records
            assertThat(updated).containsExactlyElementsOf(expected);

            // replace 1 record
            BsonDocument replacement = newDocs.get(2);
            replacement.put("f1", new BsonString("4"));
            testCollection.replaceOne(Filters.eq("_id", replacement.remove("_id")), replacement);
            List<String> replaced = fetchRows(iterator, 2);
            expected =
                    Arrays.asList(
                            "-U[5, 2, false, [3], 6, 2022-09-07T10:25:28.127Z, 2022-09-07T10:25:28Z, 0.9, 1.10, {k=12}, +I[13], [11_1, 11_2], [+I[12_1], +I[12_2]]]",
                            "+U[5, 4, false, [3], 6, 2022-09-07T10:25:28.127Z, 2022-09-07T10:25:28Z, 0.9, 1.10, {k=12}, +I[13], [11_1, 11_2], [+I[12_1], +I[12_2]]]");
            // assert replaced records
            assertThat(replaced).containsExactlyElementsOf(expected);
        }
    }

    @TestTemplate
    public void testProject() {
        tEnv.executeSql(createTestDDl(null));

        Iterator<Row> collected = tEnv.executeSql("SELECT f1, f10 FROM mongo_source").collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        List<String> expected =
                Stream.of("+I[2, +I[13]]", "+I[2, +I[13]]").sorted().collect(Collectors.toList());

        assertThat(result).isEqualTo(expected);
    }

    @TestTemplate
    public void testLimit() {
        tEnv.executeSql(createTestDDl(null));

        Iterator<Row> collected = tEnv.executeSql("SELECT * FROM mongo_source LIMIT 1").collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        Set<String> expected = new HashSet<>();
        expected.add(
                "+I[1, 2, false, [3], 6, 2022-09-07T10:25:28.127Z, 2022-09-07T10:25:28Z, 0.9, 1.10, {k=12}, +I[13], [11_1, 11_2], [+I[12_1], +I[12_2]]]");
        expected.add(
                "+I[2, 2, false, [3], 6, 2022-09-07T10:25:28.127Z, 2022-09-07T10:25:28Z, 0.9, 1.10, {k=12}, +I[13], [11_1, 11_2], [+I[12_1], +I[12_2]]]");

        assertThat(result).hasSize(1);
        assertThat(result).containsAnyElementsOf(expected);
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
                        "  f3 BINARY,",
                        "  f4 INTEGER,",
                        "  f5 TIMESTAMP_LTZ(6),",
                        "  f6 TIMESTAMP_LTZ(3),",
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

    private static BsonDocument createTestData(long id) {
        return new BsonDocument()
                .append("_id", new BsonInt64(id))
                .append("f1", new BsonString("2"))
                .append("f2", BsonBoolean.FALSE)
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
                                Arrays.asList(new BsonString("11_1"), new BsonString("11_2"))))
                .append(
                        "f12",
                        new BsonArray(
                                Arrays.asList(
                                        new BsonDocument("k", new BsonString("12_1")),
                                        new BsonDocument("k", new BsonString("12_2")))));
    }

    private static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Row row = iter.next();
            rows.add(row.toString());
            size--;
        }
        return rows;
    }
}
