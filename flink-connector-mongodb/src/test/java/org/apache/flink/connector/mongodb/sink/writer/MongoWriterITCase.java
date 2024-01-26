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

package org.apache.flink.connector.mongodb.sink.writer;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.connector.mongodb.sink.config.MongoWriteOptions;
import org.apache.flink.connector.mongodb.sink.writer.context.MongoSinkContext;
import org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema;
import org.apache.flink.connector.mongodb.testutils.MongoTestUtil;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Optional;

import static org.apache.flink.connector.mongodb.testutils.MongoTestUtil.assertThatIdsAreNotWritten;
import static org.apache.flink.connector.mongodb.testutils.MongoTestUtil.assertThatIdsAreWritten;
import static org.apache.flink.connector.mongodb.testutils.MongoTestUtil.assertThatIdsAreWrittenWithMaxWaitTime;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link MongoWriter}. */
@Testcontainers
public class MongoWriterITCase {

    private static final Logger LOG = LoggerFactory.getLogger(MongoWriterITCase.class);

    private static final String TEST_DATABASE = "test_writer";

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .build());

    @Container
    private static final MongoDBContainer MONGO_CONTAINER =
            MongoTestUtil.createMongoDBContainer(LOG);

    private static MongoClient mongoClient;
    private static TestSinkInitContext sinkInitContext;

    @BeforeAll
    static void beforeAll() {
        mongoClient = MongoClients.create(MONGO_CONTAINER.getConnectionString());
    }

    @AfterAll
    static void afterAll() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @BeforeEach
    void setUp() {
        sinkInitContext = new TestSinkInitContext();
    }

    @Test
    void testWriteOnBulkFlush() throws Exception {
        final String collection = "test-bulk-flush-without-checkpoint";
        final boolean flushOnCheckpoint = false;
        final int batchSize = 5;
        final int batchIntervalMs = -1;

        try (final MongoWriter<Document> writer =
                createWriter(collection, batchSize, batchIntervalMs, flushOnCheckpoint)) {
            writer.write(buildMessage(1), null);
            writer.write(buildMessage(2), null);
            writer.write(buildMessage(3), null);
            writer.write(buildMessage(4), null);

            // Ignore flush on checkpoint
            writer.flush(false);

            assertThatIdsAreNotWritten(collectionOf(collection), 1, 2, 3, 4);

            // Trigger flush
            writer.write(buildMessage(5), null);
            assertThatIdsAreWritten(collectionOf(collection), 1, 2, 3, 4, 5);

            writer.write(buildMessage(6), null);
            assertThatIdsAreNotWritten(collectionOf(collection), 6);

            // Force flush
            writer.doBulkWrite();
            assertThatIdsAreWritten(collectionOf(collection), 1, 2, 3, 4, 5, 6);
        }
    }

    @Test
    void testWriteOnBatchIntervalFlush() throws Exception {
        final String collection = "test-bulk-flush-with-interval";
        final boolean flushOnCheckpoint = false;
        final int batchSize = -1;
        final int batchIntervalMs = 1000;

        try (final MongoWriter<Document> writer =
                createWriter(collection, batchSize, batchIntervalMs, flushOnCheckpoint)) {
            writer.write(buildMessage(1), null);
            writer.write(buildMessage(2), null);
            writer.doBulkWrite();
            writer.write(buildMessage(3), null);
            writer.write(buildMessage(4), null);

            assertThatIdsAreWrittenWithMaxWaitTime(collectionOf(collection), 10000L, 1, 2, 3, 4);
        }
    }

    @Test
    void testWriteOnCheckpoint() throws Exception {
        final String collection = "test-bulk-flush-with-checkpoint";
        final boolean flushOnCheckpoint = true;
        final int batchSize = -1;
        final int batchIntervalMs = -1;

        // Enable flush on checkpoint
        try (final MongoWriter<Document> writer =
                createWriter(collection, batchSize, batchIntervalMs, flushOnCheckpoint)) {
            writer.write(buildMessage(1), null);
            writer.write(buildMessage(2), null);
            writer.write(buildMessage(3), null);

            assertThatIdsAreNotWritten(collectionOf(collection), 1, 2, 3);

            // Trigger flush
            writer.flush(false);

            assertThatIdsAreWritten(collectionOf(collection), 1, 2, 3);
        }
    }

    @Test
    void testIncrementRecordsSendMetric() throws Exception {
        final String collection = "test-inc-records-send";
        final boolean flushOnCheckpoint = false;
        final int batchSize = 2;
        final int batchIntervalMs = -1;

        try (final MongoWriter<Document> writer =
                createWriter(collection, batchSize, batchIntervalMs, flushOnCheckpoint)) {
            final Counter recordsSend = sinkInitContext.getNumRecordsOutCounter();

            writer.write(buildMessage(1), null);
            // Update existing index
            writer.write(buildMessage(2, "u"), null);
            // Delete index
            writer.write(buildMessage(3, "d"), null);

            writer.doBulkWrite();

            assertThat(recordsSend.getCount()).isEqualTo(3L);
        }
    }

    @Test
    void testCurrentSendTime() throws Exception {
        final String collection = "test-current-send-time";
        final boolean flushOnCheckpoint = false;
        final int batchSize = 1;
        final int batchIntervalMs = -1;
        final int retryTimes = 5;

        try (final MongoWriter<Document> writer =
                createWriter(collection, batchSize, batchIntervalMs, flushOnCheckpoint)) {
            final Optional<Gauge<Long>> currentSendTime = sinkInitContext.getCurrentSendTimeGauge();

            assertThat(currentSendTime.isPresent()).isTrue();
            assertThat(currentSendTime.get().getValue()).isEqualTo(Long.MAX_VALUE);

            // Since currentTimeMillis does not guarantee a monotonous sequence,
            // we added a retry mechanism to make the tests more stable.
            for (int i = 0; i < retryTimes; i++) {
                writer.write(buildMessage(i), null);
                writer.doBulkWrite();

                // currentSendTime should be larger than 0.
                if (currentSendTime.get().getValue() > 0L) {
                    return;
                }
            }

            fail("Test currentSendTime should be larger than 0 failed over max retry times.");
        }
    }

    @Test
    void testSinkContext() throws Exception {
        final String collection = "test-sink-context";
        final boolean flushOnCheckpoint = false;
        final int batchSize = 2;
        final int batchIntervalMs = -1;

        MongoWriteOptions expectOptions =
                MongoWriteOptions.builder()
                        .setBatchSize(batchSize)
                        .setBatchIntervalMs(batchIntervalMs)
                        .setDeliveryGuarantee(DeliveryGuarantee.NONE)
                        .build();

        MongoSerializationSchema<Document> testSerializationSchema =
                (element, context) -> {
                    assertThat(context.getInitContext().getSubtaskId()).isEqualTo(0);
                    assertThat(context.getWriteOptions()).isEqualTo(expectOptions);
                    assertThat(context.processTime())
                            .isEqualTo(
                                    sinkInitContext
                                            .getProcessingTimeService()
                                            .getCurrentProcessingTime());
                    return new InsertOneModel<>(element.toBsonDocument());
                };

        try (MongoWriter<Document> writer =
                createWriter(
                        collection,
                        batchSize,
                        batchIntervalMs,
                        flushOnCheckpoint,
                        testSerializationSchema)) {
            writer.write(buildMessage(1), null);
            writer.write(buildMessage(2), null);

            writer.doBulkWrite();
        }
    }

    private static MongoCollection<Document> collectionOf(String collection) {
        return mongoClient.getDatabase(TEST_DATABASE).getCollection(collection);
    }

    private static MongoWriter<Document> createWriter(
            String collection, int batchSize, long batchIntervalMs, boolean flushOnCheckpoint) {
        return createWriter(
                collection,
                batchSize,
                batchIntervalMs,
                flushOnCheckpoint,
                new UpsertSerializationSchema());
    }

    private static MongoWriter<Document> createWriter(
            String collection,
            int batchSize,
            long batchIntervalMs,
            boolean flushOnCheckpoint,
            MongoSerializationSchema<Document> serializationSchema) {

        MongoSink<Document> mongoSink =
                MongoSink.<Document>builder()
                        .setUri(MONGO_CONTAINER.getConnectionString())
                        .setDatabase(TEST_DATABASE)
                        .setCollection(collection)
                        .setBatchSize(batchSize)
                        .setBatchIntervalMs(batchIntervalMs)
                        .setDeliveryGuarantee(
                                flushOnCheckpoint
                                        ? DeliveryGuarantee.AT_LEAST_ONCE
                                        : DeliveryGuarantee.NONE)
                        .setSerializationSchema(serializationSchema)
                        .build();

        return (MongoWriter<Document>) mongoSink.createWriter(sinkInitContext);
    }

    private static Document buildMessage(int id) {
        return buildMessage(id, "i");
    }

    private static Document buildMessage(int id, String op) {
        return new Document("_id", id).append("op", op);
    }

    private static class UpsertSerializationSchema implements MongoSerializationSchema<Document> {

        @Override
        public WriteModel<BsonDocument> serialize(Document element, MongoSinkContext sinkContext) {
            String operation = element.getString("op");
            switch (operation) {
                case "i":
                    return new InsertOneModel<>(element.toBsonDocument());
                case "u":
                    {
                        BsonDocument document = element.toBsonDocument();
                        BsonDocument filter = new BsonDocument("_id", document.getInt32("_id"));
                        // _id is immutable so we remove it here to prevent exception.
                        document.remove("_id");
                        BsonDocument update = new BsonDocument("$set", document);
                        return new UpdateOneModel<>(
                                filter, update, new UpdateOptions().upsert(true));
                    }
                case "d":
                    {
                        BsonDocument document = element.toBsonDocument();
                        BsonDocument filter = new BsonDocument("_id", document.getInt32("_id"));
                        return new DeleteOneModel<>(filter);
                    }
                default:
                    throw new UnsupportedOperationException("op is not supported " + operation);
            }
        }
    }
}
