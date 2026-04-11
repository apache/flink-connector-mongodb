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
import org.apache.flink.connector.mongodb.common.config.MongoConnectionOptions;
import org.apache.flink.connector.mongodb.sink.config.DuplicateKeyStrategy;
import org.apache.flink.connector.mongodb.sink.config.MongoWriteOptions;
import org.apache.flink.connector.mongodb.sink.writer.context.MongoSinkContext;
import org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema;
import org.apache.flink.connector.mongodb.testutils.MongoTestUtil;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.InsertOneModel;
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
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link MongoWriter} duplicate key error handling with {@link
 * DuplicateKeyStrategy#SKIP_DUPLICATES} mode.
 *
 * <p>Validates that the writer correctly handles DuplicateKeyException (E11000) during
 * AT_LEAST_ONCE replay after recovery, where records with unique indexes may already exist in
 * MongoDB.
 */
@Testcontainers
class MongoWriterDuplicateKeyITCase {

    private static final Logger LOG = LoggerFactory.getLogger(MongoWriterDuplicateKeyITCase.class);

    private static final String TEST_DATABASE = "test_duplicate_key";

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .build());

    @Container
    private static final MongoDBContainer MONGO_CONTAINER =
            MongoTestUtil.createMongoDBContainer().withLogConsumer(new Slf4jLogConsumer(LOG));

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
    void testAllDuplicateKeysAreSkipped() throws Exception {
        final String collection = "test-all-duplicates-skipped";

        createUniqueIndex(collection, "recordId");

        // First write: insert records 1-5
        try (MongoWriter<Document> writer = createWriter(collection, 10, -1, true)) {
            for (int i = 1; i <= 5; i++) {
                writer.write(buildMessage(i), null);
            }
            writer.flush(false);
        }
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(5);

        // Second write: replay the same records (simulates AT_LEAST_ONCE recovery)
        try (MongoWriter<Document> writer = createWriter(collection, 10, -1, true)) {
            for (int i = 1; i <= 5; i++) {
                writer.write(buildMessage(i), null);
            }
            // Should NOT throw — duplicates are skipped
            writer.flush(false);
        }
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(5);
    }

    @Test
    void testMixedDuplicatesAndNewRecords() throws Exception {
        final String collection = "test-mixed-duplicates-new";

        createUniqueIndex(collection, "recordId");

        // First write: insert records 1-5
        try (MongoWriter<Document> writer = createWriter(collection, 10, -1, true)) {
            for (int i = 1; i <= 5; i++) {
                writer.write(buildMessage(i), null);
            }
            writer.flush(false);
        }
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(5);

        // Second write: replay records 3-10 (3-5 are duplicates, 6-10 are new)
        try (MongoWriter<Document> writer = createWriter(collection, 10, -1, true)) {
            for (int i = 3; i <= 10; i++) {
                writer.write(buildMessage(i), null);
            }
            writer.flush(false);
        }
        // All 10 records should exist
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(10);
        assertRecordIdsExist(collection, 1, 10);
    }

    @Test
    void testOrderedWritesDuplicateInMiddleOfBatch() throws Exception {
        final String collection = "test-ordered-dup-middle";

        createUniqueIndex(collection, "recordId");

        // Pre-insert record 3 to create a duplicate in the middle of the next batch
        try (MongoWriter<Document> writer = createWriter(collection, 10, -1, true)) {
            writer.write(buildMessage(3), null);
            writer.flush(false);
        }
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(1);

        // Write records 1-10 with ordered=true (default).
        // Record 3 will hit DuplicateKeyException. With ordered=true, MongoDB stops
        // at the first error — records 1-2 succeed, record 3 fails, records 4-10
        // are never attempted. The fix must re-queue records 4-10.
        try (MongoWriter<Document> writer = createWriter(collection, 20, -1, true)) {
            for (int i = 1; i <= 10; i++) {
                writer.write(buildMessage(i), null);
            }
            writer.flush(false);
        }
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(10);
        assertRecordIdsExist(collection, 1, 10);
    }

    @Test
    void testOrderedWritesMultipleDuplicatesInBatch() throws Exception {
        final String collection = "test-ordered-multi-dup";

        createUniqueIndex(collection, "recordId");

        // Pre-insert records 2, 5, 8 to create multiple duplicates
        try (MongoWriter<Document> writer = createWriter(collection, 10, -1, true)) {
            writer.write(buildMessage(2), null);
            writer.write(buildMessage(5), null);
            writer.write(buildMessage(8), null);
            writer.flush(false);
        }
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(3);

        // Write records 1-10. With ordered=true, each duplicate stops the batch.
        // The fix must strip duplicates and re-queue un-attempted records each time.
        try (MongoWriter<Document> writer = createWriter(collection, 20, -1, true)) {
            for (int i = 1; i <= 10; i++) {
                writer.write(buildMessage(i), null);
            }
            writer.flush(false);
        }
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(10);
        assertRecordIdsExist(collection, 1, 10);
    }

    @Test
    void testUnorderedWritesDuplicatesHandled() throws Exception {
        final String collection = "test-unordered-duplicates";

        createUniqueIndex(collection, "recordId");

        // Pre-insert records 2, 4, 6
        try (MongoWriter<Document> writer = createWriter(collection, 10, -1, true, false)) {
            writer.write(buildMessage(2), null);
            writer.write(buildMessage(4), null);
            writer.write(buildMessage(6), null);
            writer.flush(false);
        }
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(3);

        // Write records 1-8 with ordered=false.
        // With unordered writes, MongoDB attempts all operations — non-duplicate
        // records succeed even when some fail with E11000.
        try (MongoWriter<Document> writer = createWriter(collection, 10, -1, true, false)) {
            for (int i = 1; i <= 8; i++) {
                writer.write(buildMessage(i), null);
            }
            writer.flush(false);
        }
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(8);
        assertRecordIdsExist(collection, 1, 8);
    }

    @Test
    void testLargeBatchWithManyDuplicates() throws Exception {
        final String collection = "test-large-batch-duplicates";

        createUniqueIndex(collection, "recordId");

        // First write: insert records 1-50
        try (MongoWriter<Document> writer = createWriter(collection, 100, -1, true)) {
            for (int i = 1; i <= 50; i++) {
                writer.write(buildMessage(i), null);
            }
            writer.flush(false);
        }
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(50);

        // Second write: replay records 1-100 (1-50 are duplicates, 51-100 are new)
        // This tests that the retry counter doesn't exhaust maxRetries when there
        // are many consecutive duplicates in an ordered batch.
        try (MongoWriter<Document> writer = createWriter(collection, 200, -1, true)) {
            for (int i = 1; i <= 100; i++) {
                writer.write(buildMessage(i), null);
            }
            writer.flush(false);
        }
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(100);
        assertRecordIdsExist(collection, 1, 100);
    }

    /**
     * Verifies that unordered writes do not re-queue already-succeeded records.
     *
     * <p>With {@code ordered=false}, MongoDB attempts ALL operations in the batch. Non-duplicate
     * records succeed in the same {@code bulkWrite} call. The handler must NOT re-queue operations
     * after {@code maxErrorIndex} — unlike ordered writes, those records were already attempted and
     * succeeded.
     *
     * <p>This test uses a collection WITHOUT a unique index on {@code recordId} (only the default
     * unique {@code _id} index). If the fix incorrectly re-queues already-succeeded records, the
     * re-inserted documents get NEW {@code _id} values (since we use a schema that does not set
     * {@code _id}), creating actual duplicate documents.
     */
    @Test
    void testUnorderedWritesDoNotReQueueSucceededRecords() throws Exception {
        final String collection = "test-unordered-no-requeue";

        // Unique index on recordId — so we can cause E11000 on recordId,
        // while _id is auto-generated (different on each insert attempt).
        createUniqueIndex(collection, "recordId");

        // Pre-insert records 2, 5 to cause duplicate key errors in the middle of batch
        collectionOf(collection)
                .insertOne(new Document("recordId", 2).append("value", "pre-existing"));
        collectionOf(collection)
                .insertOne(new Document("recordId", 5).append("value", "pre-existing"));
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(2);

        // Write 8 records with ordered=false.
        // MongoDB attempts ALL 8. Records 2,5 fail (E11000 on recordId).
        // Records 1,3,4,6,7,8 succeed in the SAME bulkWrite call.
        //
        // Correct behavior: skip the 2 duplicates, done. Total = 8 documents.
        //
        // Without this fix, maxErrorIndex=4 (record 5), so records at indices
        // 5,6,7 (records 6,7,8) would be re-queued. But they already have
        // auto-generated _id set by the driver from the first call.
        // Re-inserting them would cause E11000 on _id (not on recordId),
        // wasting retry cycles or creating duplicate documents.
        try (MongoWriter<Document> writer = createWriter(collection, 10, -1, true, false)) {
            for (int i = 1; i <= 8; i++) {
                writer.write(buildMessage(i), null);
            }
            writer.flush(false);
        }

        // Verify exactly 8 documents — no duplicates
        assertThat(collectionOf(collection).countDocuments())
                .as("Should be exactly 8 documents with no unnecessary re-queuing")
                .isEqualTo(8);
        assertRecordIdsExist(collection, 1, 8);
    }

    @Test
    void testOrderedWritesDuplicateAtFirstRecord() throws Exception {
        final String collection = "test-ordered-dup-first";

        createUniqueIndex(collection, "recordId");

        // Pre-insert record 1 so the very first operation in the batch hits E11000
        try (MongoWriter<Document> writer = createWriter(collection, 20, -1, true)) {
            writer.write(buildMessage(1), null);
            writer.flush(false);
        }
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(1);

        // Write records 1-5 with ordered=true.
        // Record 1 (index 0) hits E11000. MongoDB stops immediately.
        // Records 2-5 are never attempted. The fix must re-queue all of them.
        try (MongoWriter<Document> writer = createWriter(collection, 20, -1, true)) {
            for (int i = 1; i <= 5; i++) {
                writer.write(buildMessage(i), null);
            }
            writer.flush(false);
        }
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(5);
        assertRecordIdsExist(collection, 1, 5);
    }

    @Test
    void testOrderedWritesDuplicateAtLastRecord() throws Exception {
        final String collection = "test-ordered-dup-last";

        createUniqueIndex(collection, "recordId");

        // Pre-insert record 5 so the last operation in the batch hits E11000
        try (MongoWriter<Document> writer = createWriter(collection, 20, -1, true)) {
            writer.write(buildMessage(5), null);
            writer.flush(false);
        }
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(1);

        // Write records 1-5. Records 1-4 succeed, record 5 hits E11000.
        // unattempted == 0, so nothing to re-queue. Should complete.
        try (MongoWriter<Document> writer = createWriter(collection, 20, -1, true)) {
            for (int i = 1; i <= 5; i++) {
                writer.write(buildMessage(i), null);
            }
            writer.flush(false);
        }
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(5);
        assertRecordIdsExist(collection, 1, 5);
    }

    @Test
    void testSingleRecordDuplicate() throws Exception {
        final String collection = "test-single-record-dup";

        createUniqueIndex(collection, "recordId");

        // Pre-insert record 1
        try (MongoWriter<Document> writer = createWriter(collection, 10, -1, true)) {
            writer.write(buildMessage(1), null);
            writer.flush(false);
        }
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(1);

        // Write a single duplicate record — should be skipped without error
        try (MongoWriter<Document> writer = createWriter(collection, 10, -1, true)) {
            writer.write(buildMessage(1), null);
            writer.flush(false);
        }
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(1);
    }

    @Test
    void testSkipDuplicatesWithZeroMaxRetries() throws Exception {
        final String collection = "test-zero-retries";

        createUniqueIndex(collection, "recordId");

        // Pre-insert records 1-3
        try (MongoWriter<Document> writer = createWriter(collection, 10, -1, true)) {
            for (int i = 1; i <= 3; i++) {
                writer.write(buildMessage(i), null);
            }
            writer.flush(false);
        }
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(3);

        // Write records 1-6 with maxRetries=0 and SKIP_DUPLICATES.
        // The i-- counter manipulation must not cause issues with maxRetries=0.
        MongoConnectionOptions connectionOptions =
                MongoConnectionOptions.builder()
                        .setUri(MONGO_CONTAINER.getConnectionString())
                        .setDatabase(TEST_DATABASE)
                        .setCollection(collection)
                        .build();

        MongoWriteOptions writeOptions =
                MongoWriteOptions.builder()
                        .setBatchSize(20)
                        .setBatchIntervalMs(-1)
                        .setMaxRetries(0)
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setOrderedWrites(true)
                        .setDuplicateKeyStrategy(DuplicateKeyStrategy.SKIP_DUPLICATES)
                        .build();

        try (MongoWriter<Document> writer =
                new MongoWriter<>(
                        connectionOptions,
                        writeOptions,
                        true,
                        sinkInitContext,
                        new InsertSerializationSchema())) {
            for (int i = 1; i <= 6; i++) {
                writer.write(buildMessage(i), null);
            }
            writer.flush(false);
        }
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(6);
        assertRecordIdsExist(collection, 1, 6);
    }

    @Test
    void testMixedErrorsDuplicateAndValidation() throws Exception {
        final String collection = "test-mixed-errors";

        // Create collection with a JSON schema validator that requires recordId to be
        // an integer. This lets us trigger validation errors (code 121) alongside
        // duplicate key errors (code 11000).
        mongoClient
                .getDatabase(TEST_DATABASE)
                .createCollection(
                        collection,
                        new com.mongodb.client.model.CreateCollectionOptions()
                                .validationOptions(
                                        new com.mongodb.client.model.ValidationOptions()
                                                .validator(
                                                        com.mongodb.client.model.Filters.type(
                                                                "recordId", "int"))));
        createUniqueIndex(collection, "recordId");

        // Pre-insert record 2 to cause a duplicate
        collectionOf(collection)
                .insertOne(new Document("recordId", 2).append("value", "pre-existing"));
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(1);

        // Write records with ordered=false:
        // record 1: OK (new, valid)
        // record 2: E11000 (duplicate key)
        // record 3: validation error (recordId is a string, not int)
        // record 4: OK (new, valid)
        //
        // With ordered=false, MongoDB attempts all. Expect E11000 + validation error.
        // handleMixedErrors should: skip duplicate (record 2), retry validation
        // error (record 3). Records 1 and 4 already succeeded.
        MongoWriter<Document> writer = createWriter(collection, 10, -1, true, false);
        try {
            writer.write(buildMessage(1), null);
            writer.write(buildMessage(2), null);
            // Record with invalid type for recordId — triggers validation error
            writer.write(new Document("recordId", "not-an-int").append("value", "invalid"), null);
            writer.write(buildMessage(4), null);

            // This will retry record 3 (validation error) which will fail again,
            // eventually throwing IOException after maxRetries
            assertThatThrownBy(() -> writer.flush(false)).isInstanceOf(IOException.class);
        } finally {
            try {
                writer.close();
            } catch (Exception ignored) {
            }
        }

        // Records 1 and 4 should have been written (succeeded in unordered batch).
        // Record 2 was pre-existing. Record 3 failed validation.
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(3);
        assertRecordIdsExist(collection, 1, 2);
        assertRecordIdsExist(collection, 4, 4);
    }

    @Test
    void testMixedErrorsDuplicateAndValidationOrdered() throws Exception {
        final String collection = "test-mixed-errors-ordered";

        // Create collection with a JSON schema validator that requires recordId to be
        // an integer. This lets us trigger validation errors (code 121) alongside
        // duplicate key errors (code 11000).
        mongoClient
                .getDatabase(TEST_DATABASE)
                .createCollection(
                        collection,
                        new com.mongodb.client.model.CreateCollectionOptions()
                                .validationOptions(
                                        new com.mongodb.client.model.ValidationOptions()
                                                .validator(
                                                        com.mongodb.client.model.Filters.type(
                                                                "recordId", "int"))));
        createUniqueIndex(collection, "recordId");

        // Pre-insert record 2 to cause a duplicate
        collectionOf(collection)
                .insertOne(new Document("recordId", 2).append("value", "pre-existing"));
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(1);

        // Write records with ordered=true:
        // record 1: OK (new, valid)
        // record 2: E11000 (duplicate key) — MongoDB stops here for ordered writes
        // record 3: validation error (would fail, but never attempted due to ordered)
        // record 4: OK (new, valid — never attempted due to ordered)
        //
        // With ordered=true, MongoDB stops at record 2. Records 3-4 are un-attempted.
        // handleBulkWriteException should: strip duplicate (record 2), re-queue
        // records 3-4 as un-attempted ops.
        // Next iteration: record 3 triggers validation error, record 4 un-attempted.
        // handleMixedErrors should: retry record 3 (validation), re-queue record 4.
        // Record 3 keeps failing — eventually throws IOException after maxRetries.
        MongoWriter<Document> writer = createWriter(collection, 10, -1, true, true);
        try {
            writer.write(buildMessage(1), null);
            writer.write(buildMessage(2), null);
            // Record with invalid type for recordId — triggers validation error
            writer.write(new Document("recordId", "not-an-int").append("value", "invalid"), null);
            writer.write(buildMessage(4), null);

            // Eventually throws IOException because record 3 (validation error) can
            // never succeed, exhausting maxRetries
            assertThatThrownBy(() -> writer.flush(false)).isInstanceOf(IOException.class);
        } finally {
            try {
                writer.close();
            } catch (Exception ignored) {
            }
        }

        // Record 1 succeeded. Record 2 was pre-existing (duplicate skipped).
        // Record 3 failed validation permanently. Record 4 was re-queued but could
        // never be reached because ordered=true means record 3 always blocks it.
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(2);
        assertRecordIdsExist(collection, 1, 2);
    }

    private void createUniqueIndex(String collection, String field) {
        MongoTestUtil.createIndex(
                mongoClient,
                TEST_DATABASE,
                collection,
                Indexes.ascending(field),
                new IndexOptions().unique(true));
    }

    /**
     * Verifies that FAIL mode (default) throws IOException with a descriptive error message when
     * duplicate key errors are encountered. The error message must suggest using 'skip-duplicates'
     * or upsert mode so users know how to resolve the issue.
     *
     * <p>This test ensures backward compatibility: the default behavior must always throw on
     * duplicate keys, never silently skip records.
     */
    @Test
    void testFailModeThrowsDescriptiveExceptionOnDuplicateKeys() throws Exception {
        final String collection = "test-fail-mode-throws";
        createUniqueIndex(collection, "recordId");

        // Insert record 1 successfully
        try (MongoWriter<Document> writer = createWriterWithFailStrategy(collection)) {
            writer.write(buildMessage(1), null);
            writer.flush(false);
        }
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(1);

        // Write the same record again — should throw IOException with descriptive message
        MongoWriter<Document> writer = createWriterWithFailStrategy(collection);
        try {
            writer.write(buildMessage(1), null);

            assertThatThrownBy(() -> writer.flush(false))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("duplicate key error (E11000)")
                    .hasMessageContaining("skip-duplicates");
        } finally {
            try {
                writer.close();
            } catch (Exception ignored) {
            }
        }

        // Only the original record should exist
        assertThat(collectionOf(collection).countDocuments()).isEqualTo(1);
    }

    private MongoCollection<Document> collectionOf(String collection) {
        return mongoClient.getDatabase(TEST_DATABASE).getCollection(collection);
    }

    private void assertRecordIdsExist(String collection, int from, int to) {
        List<Integer> actualIds = new ArrayList<>();
        collectionOf(collection).find().map(d -> d.getInteger("recordId")).into(actualIds);
        for (int i = from; i <= to; i++) {
            assertThat(actualIds).contains(i);
        }
    }

    private MongoWriter<Document> createWriter(
            String collection, int batchSize, long batchIntervalMs, boolean flushOnCheckpoint)
            throws Exception {
        return createWriter(collection, batchSize, batchIntervalMs, flushOnCheckpoint, true);
    }

    private MongoWriter<Document> createWriter(
            String collection,
            int batchSize,
            long batchIntervalMs,
            boolean flushOnCheckpoint,
            boolean ordered)
            throws Exception {

        MongoConnectionOptions connectionOptions =
                MongoConnectionOptions.builder()
                        .setUri(MONGO_CONTAINER.getConnectionString())
                        .setDatabase(TEST_DATABASE)
                        .setCollection(collection)
                        .build();

        MongoWriteOptions writeOptions =
                MongoWriteOptions.builder()
                        .setBatchSize(batchSize)
                        .setBatchIntervalMs(batchIntervalMs)
                        .setMaxRetries(3)
                        .setDeliveryGuarantee(
                                flushOnCheckpoint
                                        ? DeliveryGuarantee.AT_LEAST_ONCE
                                        : DeliveryGuarantee.NONE)
                        .setOrderedWrites(ordered)
                        .setDuplicateKeyStrategy(DuplicateKeyStrategy.SKIP_DUPLICATES)
                        .build();

        return new MongoWriter<>(
                connectionOptions,
                writeOptions,
                flushOnCheckpoint,
                sinkInitContext,
                new InsertSerializationSchema());
    }

    private MongoWriter<Document> createWriterWithFailStrategy(String collection) {

        MongoConnectionOptions connectionOptions =
                MongoConnectionOptions.builder()
                        .setUri(MONGO_CONTAINER.getConnectionString())
                        .setDatabase(TEST_DATABASE)
                        .setCollection(collection)
                        .build();

        MongoWriteOptions writeOptions =
                MongoWriteOptions.builder()
                        .setBatchSize(10)
                        .setBatchIntervalMs(-1)
                        .setMaxRetries(1)
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setDuplicateKeyStrategy(DuplicateKeyStrategy.FAIL)
                        .build();

        return new MongoWriter<>(
                connectionOptions,
                writeOptions,
                true,
                sinkInitContext,
                new InsertSerializationSchema());
    }

    private static Document buildMessage(int recordId) {
        return new Document("recordId", recordId).append("value", "data-" + recordId);
    }

    private static class InsertSerializationSchema implements MongoSerializationSchema<Document> {

        @Override
        public WriteModel<BsonDocument> serialize(Document element, MongoSinkContext sinkContext) {
            return new InsertOneModel<>(element.toBsonDocument());
        }
    }
}
