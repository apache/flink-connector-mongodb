/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDbPointer;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static org.apache.flink.table.api.Expressions.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT tests for {@link MongoDynamicTableSink}. */
@Testcontainers
class MongoDynamicTableSinkITCase {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDynamicTableSinkITCase.class);

    @Container
    private static final MongoDBContainer MONGO_CONTAINER =
            MongoTestUtil.createMongoDBContainer().withLogConsumer(new Slf4jLogConsumer(LOG));

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .build());

    private MongoClient mongoClient;

    @BeforeEach
    void setUp() {
        mongoClient = MongoClients.create(MONGO_CONTAINER.getConnectionString());
    }

    @AfterEach
    void tearDown() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Test
    void testSinkWithAllSupportedTypes() throws ExecutionException, InterruptedException {
        String database = "test";
        String collection = "sink_with_all_supported_types";

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tEnv.executeSql(
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE TABLE mongo_sink",
                                "(",
                                "  _id BIGINT,",
                                "  f1 STRING,",
                                "  f2 BOOLEAN,",
                                "  f3 BINARY,",
                                "  f4 INTEGER,",
                                "  f5 TIMESTAMP_LTZ(6),",
                                "  f6 TIMESTAMP(3),",
                                "  f7 DOUBLE,",
                                "  f8 DECIMAL(10, 2),",
                                "  f9 MAP<STRING, INTEGER>,",
                                "  f10 ROW<k INTEGER>,",
                                "  f11 ARRAY<STRING>,",
                                "  f12 ARRAY<ROW<k STRING>>,",
                                "  PRIMARY KEY (_id) NOT ENFORCED",
                                ") WITH (",
                                getConnectorSql(database, collection),
                                ")")));

        Instant now = Instant.now();
        tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("_id", DataTypes.BIGINT()),
                                DataTypes.FIELD("f1", DataTypes.STRING()),
                                DataTypes.FIELD("f2", DataTypes.BOOLEAN()),
                                DataTypes.FIELD("f3", DataTypes.BINARY(1)),
                                DataTypes.FIELD("f4", DataTypes.INT()),
                                DataTypes.FIELD("f5", DataTypes.TIMESTAMP_LTZ(6)),
                                DataTypes.FIELD("f6", DataTypes.TIMESTAMP(3)),
                                DataTypes.FIELD("f7", DataTypes.DOUBLE()),
                                DataTypes.FIELD("f8", DataTypes.DECIMAL(10, 2)),
                                DataTypes.FIELD(
                                        "f9", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                                DataTypes.FIELD(
                                        "f10",
                                        DataTypes.ROW(DataTypes.FIELD("k", DataTypes.INT()))),
                                DataTypes.FIELD("f11", DataTypes.ARRAY(DataTypes.STRING())),
                                DataTypes.FIELD(
                                        "f12",
                                        DataTypes.ARRAY(
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "K", DataTypes.STRING()))))),
                        Row.of(
                                1L,
                                "ABCDE",
                                true,
                                new byte[] {(byte) 3},
                                6,
                                now,
                                Timestamp.from(now),
                                10.10d,
                                new BigDecimal("11.11"),
                                Collections.singletonMap("k", 12),
                                Row.of(13),
                                Arrays.asList("14_1", "14_2"),
                                Arrays.asList(Row.of("15_1"), Row.of("15_2"))))
                .executeInsert("mongo_sink")
                .await();

        MongoCollection<Document> coll =
                mongoClient.getDatabase(database).getCollection(collection);

        Document actual = coll.find(Filters.eq("_id", 1L)).first();

        Document expected =
                new Document("_id", 1L)
                        .append("f1", "ABCDE")
                        .append("f2", true)
                        .append("f3", new Binary(new byte[] {(byte) 3}))
                        .append("f4", 6)
                        .append("f5", Date.from(now))
                        .append("f6", Date.from(now))
                        .append("f7", 10.10d)
                        .append("f8", new Decimal128(new BigDecimal("11.11")))
                        .append("f9", new Document("k", 12))
                        .append("f10", new Document("k", 13))
                        .append("f11", Arrays.asList("14_1", "14_2"))
                        .append(
                                "f12",
                                Arrays.asList(
                                        new Document("k", "15_1"), new Document("k", "15_2")));

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testRoundTripReadAndSink() throws ExecutionException, InterruptedException {
        String database = "test";
        String sourceCollection = "test_round_trip_source";
        String sinkCollection = "test_round_trip_sink";

        BsonDocument testData =
                new BsonDocument("f1", new BsonString("ABCDE"))
                        .append("f2", new BsonBoolean(true))
                        .append("f3", new BsonBinary(new byte[] {(byte) 3}))
                        .append("f4", new BsonInt32(32))
                        .append("f5", new BsonInt64(64L))
                        .append("f6", new BsonDouble(128.128d))
                        .append("f7", new BsonDecimal128(new Decimal128(new BigDecimal("256.256"))))
                        .append("f8", new BsonDateTime(Instant.now().toEpochMilli()))
                        .append("f9", new BsonTimestamp((int) Instant.now().getEpochSecond(), 100))
                        .append(
                                "f10",
                                new BsonRegularExpression(Pattern.compile("^9$").pattern(), "i"))
                        .append("f11", new BsonJavaScript("function() { return 10; }"))
                        .append(
                                "f12",
                                new BsonJavaScriptWithScope(
                                        "function() { return 11; }", new BsonDocument()))
                        .append("f13", new BsonDbPointer("test.test", new ObjectId()))
                        .append("f14", new BsonSymbol("symbol"))
                        .append(
                                "f15",
                                new BsonArray(Arrays.asList(new BsonInt32(1), new BsonInt32(2))))
                        .append("f16", new BsonDocument("k", new BsonInt32(32)));

        MongoCollection<BsonDocument> sourceColl =
                mongoClient
                        .getDatabase(database)
                        .getCollection(sourceCollection)
                        .withDocumentClass(BsonDocument.class);
        sourceColl.insertOne(testData);

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE mongo_source (\n"
                                + "`_id` STRING,\n"
                                + "`f1` STRING,\n"
                                + "`f2` BOOLEAN,\n"
                                + "`f3` BINARY,\n"
                                + "`f4` INTEGER,\n"
                                + "`f5` BIGINT,\n"
                                + "`f6` DOUBLE,\n"
                                + "`f7` DECIMAL(10, 3),\n"
                                + "`f8` TIMESTAMP_LTZ(3),\n"
                                + "`f9` STRING,\n"
                                + "`f10` STRING,\n"
                                + "`f11` STRING,\n"
                                + "`f12` STRING,\n"
                                + "`f13` STRING,\n"
                                + "`f14` STRING,\n"
                                + "`f15` ARRAY<INTEGER>,\n"
                                + "`f16` ROW<k INTEGER>,\n"
                                + " PRIMARY KEY (_id) NOT ENFORCED\n"
                                + ") WITH ( %s )",
                        getConnectorSql(database, sourceCollection)));

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE mongo_sink WITH ( %s ) LIKE mongo_source",
                        getConnectorSql(database, sinkCollection)));

        tEnv.executeSql("insert into mongo_sink select * from mongo_source").await();

        MongoCollection<BsonDocument> sinkColl =
                mongoClient
                        .getDatabase(database)
                        .getCollection(sinkCollection)
                        .withDocumentClass(BsonDocument.class);

        BsonDocument actual = sinkColl.find().first();

        assertThat(actual).isEqualTo(testData);
    }

    @Test
    void testSinkWithAllRowKind() throws ExecutionException, InterruptedException {
        String database = "test";
        String collection = "test_sink_with_all_row_kind";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Row> sourceStream =
                env.fromCollection(
                                Arrays.asList(
                                        Row.ofKind(RowKind.INSERT, 1L, "Alice"),
                                        Row.ofKind(RowKind.DELETE, 1L, "Alice"),
                                        Row.ofKind(RowKind.INSERT, 2L, "Bob"),
                                        Row.ofKind(RowKind.UPDATE_BEFORE, 2L, "Bob"),
                                        Row.ofKind(RowKind.UPDATE_AFTER, 2L, "Tom")))
                        .returns(
                                new RowTypeInfo(
                                        new TypeInformation[] {Types.LONG, Types.STRING},
                                        new String[] {"id", "name"}));

        Schema sourceSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("name", DataTypes.STRING())
                        .build();

        Table sourceTable = tEnv.fromChangelogStream(sourceStream, sourceSchema);
        tEnv.createTemporaryView("value_source", sourceTable);

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE mongo_sink (\n"
                                + "`_id` BIGINT,\n"
                                + "`name` STRING,\n"
                                + " PRIMARY KEY (_id) NOT ENFORCED\n"
                                + ") WITH ( %s )",
                        getConnectorSql(database, collection)));

        tEnv.executeSql("insert into mongo_sink select * from value_source").await();

        MongoCollection<Document> coll =
                mongoClient.getDatabase(database).getCollection(collection);

        List<Document> expected =
                Collections.singletonList(new Document("_id", 2L).append("name", "Tom"));

        List<Document> actual = coll.find().into(new ArrayList<>());

        assertThat(actual).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSinkWithReservedId(boolean overwrite) throws Exception {
        String database = "test";
        String collection = "sink_with_reserved_id";

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE mongo_sink ("
                                + "_id STRING NOT NULL,\n"
                                + "f1 STRING NOT NULL,\n"
                                + "PRIMARY KEY (_id) NOT ENFORCED\n"
                                + ")\n"
                                + "WITH (%s)",
                        getConnectorSql(database, collection)));

        ObjectId objectId = new ObjectId();
        tEnv.fromValues(row(objectId.toHexString(), "r1"), row("str", "r2"))
                .executeInsert("mongo_sink", overwrite)
                .await();

        MongoCollection<Document> coll =
                mongoClient.getDatabase(database).getCollection(collection);

        List<Document> actual = new ArrayList<>();
        coll.find(Filters.in("_id", objectId, "str")).into(actual);

        Document[] expected =
                new Document[] {
                    new Document("_id", objectId).append("f1", "r1"),
                    new Document("_id", "str").append("f1", "r2")
                };
        assertThat(actual).containsExactlyInAnyOrder(expected);
    }

    @Test
    void testOverwriteSinkWithoutPrimaryKey() {
        String database = "test";
        String collection = "overwrite_sink_without_primary_key";

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE mongo_sink (" + "f1 STRING NOT NULL\n" + ")\n" + "WITH (%s)",
                        getConnectorSql(database, collection)));

        assertThatThrownBy(
                        () ->
                                tEnv.fromValues(row("d1"), row("d1"))
                                        .executeInsert("mongo_sink", true)
                                        .await())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Overwrite sink requires specifying the table's primary key");
    }

    @Test
    void testSinkWithoutPrimaryKey() throws Exception {
        String database = "test";
        String collection = "sink_without_primary_key";

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE mongo_sink (" + "f1 STRING NOT NULL\n" + ")\n" + "WITH (%s)",
                        getConnectorSql(database, collection)));

        tEnv.fromValues(row("d1"), row("d1")).executeInsert("mongo_sink").await();

        MongoCollection<Document> coll =
                mongoClient.getDatabase(database).getCollection(collection);

        List<Document> actual = new ArrayList<>();
        coll.find().into(actual);

        assertThat(actual).hasSize(2);
        for (Document doc : actual) {
            assertThat(doc.get("f1")).isEqualTo("d1");
        }
    }

    @Test
    void testSinkWithNonCompositePrimaryKey() throws Exception {
        String database = "test";
        String collection = "sink_with_non_composite_pk";

        Instant now = Instant.now();
        List<Expression> testValues =
                Collections.singletonList(
                        row(2L, true, "ABCDE", 12.12d, 4, Timestamp.from(now), now));
        List<String> primaryKeys = Collections.singletonList("a");

        testSinkWithoutReservedId(database, collection, primaryKeys, testValues);

        MongoCollection<Document> coll =
                mongoClient.getDatabase(database).getCollection(collection);

        Document actual = coll.find(Filters.eq("_id", 2L)).first();
        Document expected = new Document();
        expected.put("_id", 2L);
        expected.put("a", 2L);
        expected.put("b", true);
        expected.put("c", "ABCDE");
        expected.put("d", 12.12d);
        expected.put("e", 4);
        expected.put("f", Date.from(now));
        expected.put("g", Date.from(now));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testSinkWithCompositePrimaryKey() throws Exception {
        String database = "test";
        String collection = "sink_with_composite_pk";

        Instant now = Instant.now();
        List<Expression> testValues =
                Collections.singletonList(
                        row(1L, true, "ABCDE", 12.12d, 4, Timestamp.from(now), now));
        List<String> primaryKeys = Arrays.asList("a", "c");

        testSinkWithoutReservedId(database, collection, primaryKeys, testValues);

        MongoCollection<Document> coll =
                mongoClient.getDatabase(database).getCollection(collection);

        Document compositeId = new Document();
        compositeId.put("a", 1L);
        compositeId.put("c", "ABCDE");
        Document actual = coll.find(Filters.eq("_id", compositeId)).first();

        Document expected = new Document();
        expected.put("_id", new Document(compositeId));
        expected.put("a", 1L);
        expected.put("b", true);
        expected.put("c", "ABCDE");
        expected.put("d", 12.12d);
        expected.put("e", 4);
        expected.put("f", Date.from(now));
        expected.put("g", Date.from(now));
        assertThat(actual).isEqualTo(expected);
    }

    private void testSinkWithoutReservedId(
            String database, String collection, List<String> primaryKeys, List<Expression> values)
            throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE mongo_sink ("
                                + "a BIGINT NOT NULL,\n"
                                + "b BOOLEAN,\n"
                                + "c STRING NOT NULL,\n"
                                + "d DOUBLE,\n"
                                + "e INT NOT NULL,\n"
                                + "f TIMESTAMP NOT NULL,\n"
                                + "g TIMESTAMP_LTZ NOT NULL,\n"
                                + "PRIMARY KEY (%s) NOT ENFORCED\n"
                                + ")\n"
                                + "WITH (%s)",
                        getPrimaryKeys(primaryKeys), getConnectorSql(database, collection)));

        tEnv.fromValues(values).executeInsert("mongo_sink").await();
    }

    private static String getPrimaryKeys(List<String> fieldNames) {
        return String.join(",", fieldNames);
    }

    private static String getConnectorSql(String database, String collection) {
        return String.format("'%s'='%s',\n", FactoryUtil.CONNECTOR.key(), "mongodb")
                + String.format(
                        "'%s'='%s',\n",
                        MongoConnectorOptions.URI.key(), MONGO_CONTAINER.getConnectionString())
                + String.format("'%s'='%s',\n", MongoConnectorOptions.DATABASE.key(), database)
                + String.format("'%s'='%s'\n", MongoConnectorOptions.COLLECTION.key(), collection);
    }
}
