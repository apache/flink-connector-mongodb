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

package org.apache.flink.tests.util.mongodb;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.FlinkContainersSettings;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.SQLJobSubmission;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.connector.mongodb.testutils.MongoTestUtil.MONGODB_HOSTNAME;
import static org.apache.flink.connector.mongodb.testutils.MongoTestUtil.MONGO_4_0;
import static org.apache.flink.connector.mongodb.testutils.MongoTestUtil.MONGO_IMAGE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end test for the MongoDB connectors. */
@Testcontainers
class MongoE2ECase {

    private static final Logger LOG = LoggerFactory.getLogger(MongoE2ECase.class);

    private static final Network NETWORK = Network.newNetwork();

    private static final Path SQL_CONNECTOR_MONGODB_JAR =
            ResourceTestUtils.getResource(".*mongodb.jar");

    private static final int TEST_ORDERS_INITIAL_COUNT = 5;

    @Container
    static final MongoDBContainer MONGO_CONTAINER =
            new MongoDBContainer(MONGO_IMAGE_PREFIX + MONGO_4_0)
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .withNetwork(NETWORK)
                    .withNetworkAliases(MONGODB_HOSTNAME);

    private static final TestcontainersSettings TESTCONTAINERS_SETTINGS =
            TestcontainersSettings.builder()
                    .logger(LOG)
                    .network(NETWORK)
                    .dependsOn(MONGO_CONTAINER)
                    .build();

    @RegisterExtension
    public static final FlinkContainers FLINK =
            FlinkContainers.builder()
                    .withFlinkContainersSettings(
                            FlinkContainersSettings.builder()
                                    .setConfigOption(
                                            ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,
                                            Duration.ofSeconds(1))
                                    .numTaskManagers(2)
                                    .build())
                    .withTestcontainersSettings(TESTCONTAINERS_SETTINGS)
                    .build();

    private static MongoClient mongoClient;

    @BeforeAll
    static void setUp() {
        mongoClient = MongoClients.create(MONGO_CONTAINER.getConnectionString());
    }

    @AfterAll
    static void teardown() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Test
    public void testUpsertSink() throws Exception {
        MongoDatabase db = mongoClient.getDatabase("test_upsert");

        List<Document> orders = generateOrders();
        db.getCollection("orders").insertMany(orders);

        executeSqlStatements(readSqlFile("e2e_upsert.sql"));

        List<Document> ordersBackup = readAllBackupOrders(db, orders.size());

        assertThat(ordersBackup).containsExactlyInAnyOrderElementsOf(orders);
    }

    @Test
    public void testAppendOnlySink() throws Exception {
        MongoDatabase db = mongoClient.getDatabase("test_append_only");

        List<Document> orders = generateOrders();
        db.getCollection("orders").insertMany(orders);

        executeSqlStatements(readSqlFile("e2e_append_only.sql"));

        List<Document> ordersBackup = readAllBackupOrders(db, orders.size());

        List<Document> expected = removeIdField(orders);
        assertThat(removeIdField(ordersBackup)).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testUnboundedSink() throws Exception {
        MongoDatabase db = mongoClient.getDatabase("test_unbounded");
        MongoCollection<Document> coll = db.getCollection("orders");

        List<Document> orders = generateOrders();
        coll.insertMany(orders);

        executeSqlStatements(readSqlFile("e2e_unbounded.sql"));

        // -- scan records --
        List<Document> ordersBackup = readAllBackupOrders(db, orders.size());
        assertThat(ordersBackup).containsExactlyInAnyOrderElementsOf(orders);

        // -- stream records --
        // insert 3 records
        List<Document> newOrders =
                Arrays.asList(generateOrder(6), generateOrder(7), generateOrder(8));
        coll.insertMany(newOrders);
        orders.addAll(newOrders);

        // assert inserted
        ordersBackup = readAllBackupOrders(db, orders.size());
        assertThat(ordersBackup).containsExactlyInAnyOrderElementsOf(orders);

        // update 1 record
        Document updateOrder = orders.get(0);
        coll.updateOne(Filters.eq("_id", updateOrder.get("_id")), Updates.set("quantity", 1000L));

        // replace 1 record
        Document replacement = Document.parse(orders.get(1).toJson());
        replacement.put("quantity", 1001L);
        coll.replaceOne(Filters.eq("_id", replacement.remove("_id")), replacement);

        // asert updated
        ordersBackup = readAllBackupOrders(db, orders.size());
        assertThat(ordersBackup).containsExactlyInAnyOrderElementsOf(orders);
    }

    private static List<Document> readAllBackupOrders(MongoDatabase db, int expectSize)
            throws Exception {
        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(30));
        MongoCollection<Document> coll = db.getCollection("orders_bak");
        while (deadline.hasTimeLeft()) {
            if (coll.countDocuments() < expectSize) {
                Thread.sleep(1000L);
            } else {
                break;
            }
        }
        return coll.find().into(new ArrayList<>());
    }

    private static List<Document> removeIdField(List<Document> documents) {
        return documents.stream().peek(doc -> doc.remove("_id")).collect(Collectors.toList());
    }

    private static List<Document> generateOrders() {
        List<Document> orders = new ArrayList<>();
        for (int i = 1; i <= TEST_ORDERS_INITIAL_COUNT; i++) {
            orders.add(generateOrder(i));
        }
        return orders;
    }

    private static Document generateOrder(int index) {
        return new Document("_id", new ObjectId())
                .append("code", "ORDER_" + index)
                .append("quantity", index * 10L);
    }

    private static List<String> readSqlFile(final String resourceName) throws Exception {
        return Files.readAllLines(
                Paths.get(MongoE2ECase.class.getResource("/" + resourceName).toURI()));
    }

    private static void executeSqlStatements(final List<String> sqlLines) throws Exception {
        FLINK.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines)
                        .addJars(SQL_CONNECTOR_MONGODB_JAR)
                        .build());
    }
}
