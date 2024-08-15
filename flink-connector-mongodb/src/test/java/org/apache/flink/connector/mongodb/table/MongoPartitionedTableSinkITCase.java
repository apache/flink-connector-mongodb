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

import org.apache.flink.connector.mongodb.testutils.MongoShardedContainers;
import org.apache.flink.connector.mongodb.testutils.MongoTestUtil;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.test.junit5.MiniClusterExtension;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.Network;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.connector.mongodb.testutils.MongoTestUtil.getConnectorSql;
import static org.apache.flink.table.api.Expressions.nullOf;
import static org.apache.flink.table.api.Expressions.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT tests for {@link SupportsPartitioning} feature of {@link MongoDynamicTableSink}. */
class MongoPartitionedTableSinkITCase {

    @RegisterExtension
    private static final MongoShardedContainers MONGO_SHARDED_CONTAINER =
            MongoTestUtil.createMongoDBShardedContainers(Network.newNetwork());

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .build());

    private MongoClient mongoClient;

    @BeforeEach
    void setUp() {
        mongoClient = MongoClients.create(MONGO_SHARDED_CONTAINER.getConnectionString());
    }

    @AfterEach
    void tearDown() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Test
    void testSinkIntoPartitionedTable() throws Exception {
        String database = "test";
        String collection = "sink_into_sharded_collection";

        // sink into sharded collection by unique index { b: 1, c: 1 }.
        Bson hashedIndex = BsonDocument.parse("{ b: 1, c: 1 }");
        MongoTestUtil.createIndex(
                mongoClient, database, collection, hashedIndex, new IndexOptions().unique(true));
        MongoTestUtil.shardCollection(mongoClient, database, collection, hashedIndex);

        List<Expression> testValues =
                Arrays.asList(
                        row(1L, nullOf(DataTypes.BOOLEAN()), "ABCDEF", 12.12d, 4),
                        row(1L, nullOf(DataTypes.BOOLEAN()), "ABCDEF", 12.123d, 5));
        List<String> primaryKeys = Collections.singletonList("a");

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        createPartitionedTable(tEnv, database, collection, primaryKeys, Arrays.asList("b", "c"));

        tEnv.fromValues(testValues).executeInsert("mongo_sink").await();

        MongoCollection<Document> coll =
                mongoClient.getDatabase(database).getCollection(collection);

        Document expected = new Document();
        expected.put("_id", 1L);
        expected.put("a", 1L);
        expected.put("b", null);
        expected.put("c", "ABCDEF");
        expected.put("d", 12.123d);
        expected.put("e", 5);

        assertThat(coll.find(Filters.eq("_id", 1L))).containsExactly(expected);
    }

    @Test
    void testSinkIntoPartitionedTableWithMutableShardKey() {
        String database = "test";
        String collection = "sink_into_mutable_sharded_collection";

        // sink into sharded collection by unique index { b: 1, c: 1 }.
        Bson hashedIndex = BsonDocument.parse("{ b: 1, c: 1 }");
        MongoTestUtil.createIndex(
                mongoClient, database, collection, hashedIndex, new IndexOptions().unique(true));
        MongoTestUtil.shardCollection(mongoClient, database, collection, hashedIndex);

        List<Expression> testValues =
                Arrays.asList(
                        row(1L, false, "ABCDEF", 12.12d, 4), row(1L, true, "ABCDEF", 12.123d, 5));
        List<String> primaryKeys = Collections.singletonList("a");

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        createPartitionedTable(tEnv, database, collection, primaryKeys, Arrays.asList("b", "c"));

        // update the shard key value should be failed.
        assertThatThrownBy(() -> tEnv.fromValues(testValues).executeInsert("mongo_sink").await())
                .hasStackTraceContaining("Writing records to MongoDB failed");
    }

    @Test
    void testSinkIntoHashedPartitionedTable() throws Exception {
        String database = "test";
        String collection = "sink_into_hashed_sharded_collection";

        // sink into sharded collection by hashed index { c: 'hashed' }.
        Bson hashedIndex = BsonDocument.parse("{ c: 'hashed' }");
        MongoTestUtil.createIndex(
                mongoClient, database, collection, hashedIndex, new IndexOptions());
        MongoTestUtil.shardCollection(mongoClient, database, collection, hashedIndex);

        List<Expression> testValues =
                Arrays.asList(
                        row(2L, true, "ABCDEF", 12.12d, 4), row(2L, false, "ABCDEF", 12.123d, 5));

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        createPartitionedTable(
                tEnv,
                database,
                collection,
                Collections.singletonList("a"),
                Collections.singletonList("c"));

        tEnv.fromValues(testValues).executeInsert("mongo_sink").await();

        MongoCollection<Document> coll =
                mongoClient.getDatabase(database).getCollection(collection);

        Document expected = new Document();
        expected.put("_id", 2L);
        expected.put("a", 2L);
        expected.put("b", false);
        expected.put("c", "ABCDEF");
        expected.put("d", 12.123d);
        expected.put("e", 5);

        assertThat(coll.find(Filters.eq("_id", 2L))).containsExactly(expected);
    }

    @Test
    void testSinkIntoPartitionedTableAll() throws Exception {
        String database = "test";
        String collection = "sink_into_sharded_collection_all";

        // sink into static sharded collection by unique index { b: 1, c: 1 }.
        Bson hashedIndex = BsonDocument.parse("{ b: 1, c: 1 }");
        MongoTestUtil.createIndex(
                mongoClient, database, collection, hashedIndex, new IndexOptions().unique(true));
        MongoTestUtil.shardCollection(mongoClient, database, collection, hashedIndex);

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        createPartitionedTable(
                tEnv,
                database,
                collection,
                Collections.singletonList("a"),
                Arrays.asList("b", "c"));

        tEnv.executeSql(
                        "INSERT INTO mongo_sink PARTITION (b='true', c='ABCDEF') SELECT 3, 12.1234, 5")
                .await();
        tEnv.executeSql(
                        "INSERT INTO mongo_sink PARTITION (b='true', c='ABCDEF') SELECT 3, 12.12345, 6")
                .await();

        MongoCollection<Document> coll =
                mongoClient.getDatabase(database).getCollection(collection);

        Document expected = new Document();
        expected.put("_id", 3L);
        expected.put("a", 3L);
        expected.put("b", true);
        expected.put("c", "ABCDEF");
        expected.put("d", 12.12345d);
        expected.put("e", 6);

        assertThat(coll.find(Filters.eq("_id", 3L))).containsExactly(expected);
    }

    @Test
    void testSinkIntoPartitionedTablePart() throws Exception {
        String database = "test";
        String collection = "sink_into_sharded_collection_part";

        // sink into static sharded collection by unique index { b: 1, c: 1 }.
        Bson hashedIndex = BsonDocument.parse("{ c: 1, b: 1 }");
        MongoTestUtil.createIndex(
                mongoClient, database, collection, hashedIndex, new IndexOptions().unique(true));
        MongoTestUtil.shardCollection(mongoClient, database, collection, hashedIndex);

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        createPartitionedTable(
                tEnv,
                database,
                collection,
                Collections.singletonList("a"),
                Arrays.asList("c", "b"));

        tEnv.executeSql(
                        "INSERT INTO mongo_sink PARTITION (c='ABCDEFG') SELECT 4, false, 12.12345, 6")
                .await();
        tEnv.executeSql(
                        "INSERT INTO mongo_sink PARTITION (c='ABCDEFG') SELECT 4, false, 12.123456, 7")
                .await();

        MongoCollection<Document> coll =
                mongoClient.getDatabase(database).getCollection(collection);

        Document expected = new Document();
        expected.put("_id", 4L);
        expected.put("a", 4L);
        expected.put("b", false);
        expected.put("c", "ABCDEFG");
        expected.put("d", 12.123456d);
        expected.put("e", 7);

        assertThat(coll.find(Filters.eq("_id", 4L))).containsExactly(expected);
    }

    private static void createPartitionedTable(
            TableEnvironment tEnv,
            String database,
            String collection,
            List<String> primaryKeys,
            Collection<String> shardKeys) {

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE mongo_sink ("
                                + "a BIGINT NOT NULL,\n"
                                + "b BOOLEAN,\n"
                                + "c STRING NOT NULL,\n"
                                + "d DOUBLE,\n"
                                + "e INT NOT NULL,\n"
                                + "PRIMARY KEY (%s) NOT ENFORCED\n"
                                + ") "
                                + "PARTITIONED BY (%s)\n"
                                + "WITH (%s)",
                        formatKeys(primaryKeys),
                        formatKeys(shardKeys),
                        getConnectorSql(
                                database,
                                collection,
                                MONGO_SHARDED_CONTAINER.getConnectionString())));
    }

    private static String formatKeys(Collection<String> fieldNames) {
        return String.join(",", fieldNames);
    }
}
