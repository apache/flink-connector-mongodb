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
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.SQLJobSubmission;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
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
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end test for the MongoDB connectors. */
@Testcontainers
class MongoE2ECase {

    private static final Logger LOG = LoggerFactory.getLogger(MongoE2ECase.class);

    private static final String MONGODB_HOSTNAME = "mongodb";

    private static final String MONGO_4_0 = "mongo:4.0.10";

    private static final Network NETWORK = Network.newNetwork();

    private static final Path SQL_CONNECTOR_MONGODB_JAR =
            ResourceTestUtils.getResource(".*mongodb.jar");

    private static final int TEST_ORDERS_COUNT = 5;

    @Container
    static final MongoDBContainer MONGO_CONTAINER =
            new MongoDBContainer(MONGO_4_0)
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
                            FlinkContainersSettings.builder().numTaskManagers(2).build())
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

        int ordersCount = 5;
        List<Document> orders = mockOrders();
        db.getCollection("orders").insertMany(orders);

        executeSqlStatements(readSqlFile("e2e_upsert.sql"));

        List<Document> ordersBackup = readAllBackupOrders(db);

        assertThat(ordersBackup).containsExactlyInAnyOrderElementsOf(orders);
    }

    @Test
    public void testAppendOnlySink() throws Exception {
        MongoDatabase db = mongoClient.getDatabase("test_append_only");

        List<Document> orders = mockOrders();
        db.getCollection("orders").insertMany(orders);

        executeSqlStatements(readSqlFile("e2e_append_only.sql"));

        List<Document> ordersBackup = readAllBackupOrders(db);

        List<Document> expected = removeIdField(orders);
        assertThat(removeIdField(ordersBackup)).containsExactlyInAnyOrderElementsOf(expected);
    }

    private static List<Document> readAllBackupOrders(MongoDatabase db)
            throws Exception {
        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(20));
        List<Document> backupOrders;
        do {
            Thread.sleep(1000);
            backupOrders = db.getCollection("orders_bak").find().into(new ArrayList<>());
        } while (deadline.hasTimeLeft() && backupOrders.size() < TEST_ORDERS_COUNT);

        return backupOrders;
    }

    private static List<Document> removeIdField(List<Document> documents) {
        return documents.stream().peek(doc -> doc.remove("_id")).collect(Collectors.toList());
    }

    private static List<Document> mockOrders() {
        List<Document> orders = new ArrayList<>();
        for (int i = 1; i <= TEST_ORDERS_COUNT; i++) {
            orders.add(
                    new Document("_id", new ObjectId())
                            .append("code", "ORDER_" + i)
                            .append("quantity", i * 10L));
        }
        return orders;
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
