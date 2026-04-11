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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.mongodb.common.config.MongoConnectionOptions;
import org.apache.flink.connector.mongodb.sink.config.DuplicateKeyStrategy;
import org.apache.flink.connector.mongodb.sink.config.MongoWriteOptions;
import org.apache.flink.connector.mongodb.sink.writer.context.DefaultMongoSinkContext;
import org.apache.flink.connector.mongodb.sink.writer.context.MongoSinkContext;
import org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.SINK_DUPLICATE_KEY_STRATEGY;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is responsible for writing records to a MongoDB collection.
 *
 * @param <IN> The type of the input elements.
 */
@Internal
public class MongoWriter<IN> implements SinkWriter<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoWriter.class);
    private static final int DUPLICATE_KEY_ERROR_CODE = 11000;

    /** Result of inspecting a {@link MongoBulkWriteException} in SKIP_DUPLICATES mode. */
    private enum HandleResult {
        /** All write errors were duplicate key errors and were skipped. Batch cleared. */
        DUPLICATES_SKIPPED,
        /** Duplicates removed and remaining operations re-queued. Retry immediately. */
        REMAINING_REQUEUED,
        /** Non-duplicate errors remain. Count against maxRetries and retry after sleep. */
        NEEDS_RETRY
    }

    private final MongoConnectionOptions connectionOptions;
    private final MongoWriteOptions writeOptions;
    private final MongoSerializationSchema<IN> serializationSchema;
    private final MongoSinkContext sinkContext;
    private final MailboxExecutor mailboxExecutor;
    private final boolean flushOnCheckpoint;
    private final List<WriteModel<BsonDocument>> bulkRequests = new ArrayList<>();
    private final Collector<WriteModel<BsonDocument>> collector;
    private final Counter numRecordsOut;
    private final MongoClient mongoClient;
    private final long batchIntervalMs;
    private final int batchSize;

    private boolean checkpointInProgress = false;
    private volatile long lastSendTime = 0L;
    private volatile long ackTime = Long.MAX_VALUE;

    private transient volatile boolean closed = false;
    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient volatile Exception flushException;

    public MongoWriter(
            MongoConnectionOptions connectionOptions,
            MongoWriteOptions writeOptions,
            boolean flushOnCheckpoint,
            WriterInitContext initContext,
            MongoSerializationSchema<IN> serializationSchema) {
        this.connectionOptions = checkNotNull(connectionOptions);
        this.writeOptions = checkNotNull(writeOptions);
        this.serializationSchema = checkNotNull(serializationSchema);
        this.flushOnCheckpoint = flushOnCheckpoint;
        this.batchIntervalMs = writeOptions.getBatchIntervalMs();
        this.batchSize = writeOptions.getBatchSize();

        checkNotNull(initContext);
        this.mailboxExecutor = checkNotNull(initContext.getMailboxExecutor());

        SinkWriterMetricGroup metricGroup = checkNotNull(initContext.metricGroup());
        metricGroup.setCurrentSendTimeGauge(() -> ackTime - lastSendTime);

        this.numRecordsOut = metricGroup.getNumRecordsSendCounter();
        this.collector = new ListCollector<>(this.bulkRequests);

        // Initialize the serialization schema.
        this.sinkContext = new DefaultMongoSinkContext(initContext, writeOptions);
        try {
            SerializationSchema.InitializationContext initializationContext =
                    initContext.asSerializationSchemaInitializationContext();
            serializationSchema.open(initializationContext, sinkContext, writeOptions);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to open the MongoEmitter", e);
        }

        // Initialize the mongo client.
        this.mongoClient = MongoClients.create(connectionOptions.getUri());

        boolean flushOnlyOnCheckpoint = batchIntervalMs == -1 && batchSize == -1;

        if (!flushOnlyOnCheckpoint && batchIntervalMs > 0) {
            this.scheduler =
                    Executors.newScheduledThreadPool(1, new ExecutorThreadFactory("mongo-writer"));

            this.scheduledFuture =
                    this.scheduler.scheduleWithFixedDelay(
                            () -> {
                                synchronized (MongoWriter.this) {
                                    if (!closed && isOverMaxBatchIntervalLimit()) {
                                        try {
                                            doBulkWrite();
                                        } catch (Exception e) {
                                            flushException = e;
                                        }
                                    }
                                }
                            },
                            batchIntervalMs,
                            batchIntervalMs,
                            TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public synchronized void write(IN element, Context context)
            throws IOException, InterruptedException {
        checkFlushException();

        // do not allow new bulk writes until all actions are flushed
        while (checkpointInProgress) {
            mailboxExecutor.yield();
        }
        WriteModel<BsonDocument> writeModel = serializationSchema.serialize(element, sinkContext);
        numRecordsOut.inc();
        collector.collect(writeModel);
        if (isOverMaxBatchSizeLimit() || isOverMaxBatchIntervalLimit()) {
            doBulkWrite();
        }
    }

    @Override
    public synchronized void flush(boolean endOfInput) throws IOException {
        checkFlushException();

        checkpointInProgress = true;
        while (!bulkRequests.isEmpty() && (flushOnCheckpoint || endOfInput)) {
            doBulkWrite();
        }
        checkpointInProgress = false;
    }

    @Override
    public synchronized void close() throws Exception {
        if (!closed) {
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
                scheduler.shutdown();
            }

            if (!bulkRequests.isEmpty()) {
                try {
                    doBulkWrite();
                } catch (Exception e) {
                    LOG.error("Writing records to MongoDB failed when closing MongoWriter", e);
                    throw new IOException("Writing records to MongoDB failed.", e);
                } finally {
                    mongoClient.close();
                    closed = true;
                }
            } else {
                mongoClient.close();
                closed = true;
            }
        }
    }

    @VisibleForTesting
    void doBulkWrite() throws IOException {
        if (bulkRequests.isEmpty()) {
            // no records to write
            return;
        }

        if (writeOptions.getDuplicateKeyStrategy() == DuplicateKeyStrategy.SKIP_DUPLICATES) {
            doBulkWriteSkipDuplicates();
        } else {
            doBulkWriteFailOnDuplicates();
        }
    }

    /**
     * Bulk write with FAIL strategy for duplicate keys. Retries the batch unchanged up to
     * maxRetries. If duplicate key errors (E11000) are detected when retries are exhausted, throws
     * an {@link IOException} with a suggestion to use SKIP_DUPLICATES or upsert mode.
     */
    private void doBulkWriteFailOnDuplicates() throws IOException {
        int maxRetries = writeOptions.getMaxRetries();
        long retryIntervalMs = writeOptions.getRetryIntervalMs();
        for (int i = 0; i <= maxRetries; i++) {
            try {
                lastSendTime = System.currentTimeMillis();
                executeBulkWrite();
                ackTime = System.currentTimeMillis();
                bulkRequests.clear();
                break;
            } catch (MongoBulkWriteException e) {
                LOG.debug("Bulk Write to MongoDB failed, retry attempt = {}", i, e);
                if (i >= maxRetries) {
                    LOG.error("Bulk Write to MongoDB failed", e);
                    if (hasDuplicateKeyError(e)) {
                        throw new IOException(
                                "Bulk write failed with duplicate key error (E11000). "
                                        + "If using AT_LEAST_ONCE delivery with insert mode, "
                                        + "consider using upsert mode or setting '"
                                        + SINK_DUPLICATE_KEY_STRATEGY.key()
                                        + "' = '"
                                        + DuplicateKeyStrategy.SKIP_DUPLICATES
                                        + "'.",
                                e);
                    }
                    throw new IOException(e);
                }
                sleepBeforeRetry(retryIntervalMs, i, e);
            } catch (MongoException e) {
                LOG.debug("Bulk Write to MongoDB failed, retry times = {}", i, e);
                if (i >= maxRetries) {
                    LOG.error("Bulk Write to MongoDB failed", e);
                    throw new IOException(e);
                }
                sleepBeforeRetry(retryIntervalMs, i, e);
            }
        }
    }

    /**
     * Bulk write with duplicate key handling (SKIP_DUPLICATES mode). Catches {@link
     * MongoBulkWriteException} and inspects per-operation write errors. Skips duplicate key errors
     * (E11000) and re-queues remaining operations for ordered writes.
     */
    private void doBulkWriteSkipDuplicates() throws IOException {
        int maxRetries = writeOptions.getMaxRetries();
        long retryIntervalMs = writeOptions.getRetryIntervalMs();
        // Safety bound: each REMAINING_REQUEUED iteration must shrink bulkRequests.
        // Track previous size to detect non-progress and prevent unbounded looping.
        int previousBatchSize = bulkRequests.size();
        for (int i = 0; i <= maxRetries; i++) {
            try {
                lastSendTime = System.currentTimeMillis();
                executeBulkWrite();
                ackTime = System.currentTimeMillis();
                bulkRequests.clear();
                break;
            } catch (MongoBulkWriteException e) {
                HandleResult result = handleBulkWriteException(e);
                if (result == HandleResult.DUPLICATES_SKIPPED) {
                    break;
                }
                if (result == HandleResult.REMAINING_REQUEUED) {
                    if (bulkRequests.isEmpty()) {
                        break;
                    }
                    if (bulkRequests.size() >= previousBatchSize) {
                        // No progress — bulkRequests did not shrink. Fail to avoid
                        // an infinite loop.
                        LOG.error(
                                "Bulk write duplicate stripping made no progress "
                                        + "(batch size {} unchanged). Failing.",
                                previousBatchSize);
                        throw new IOException(e);
                    }
                    previousBatchSize = bulkRequests.size();
                    // Don't count against maxRetries — duplicates are not transient
                    i--;
                    continue;
                }
                LOG.debug("Bulk Write to MongoDB failed, retry times = {}", i, e);
                if (i >= maxRetries) {
                    LOG.error("Bulk Write to MongoDB failed", e);
                    throw new IOException(e);
                }
                sleepBeforeRetry(retryIntervalMs, i, e);
            } catch (MongoException e) {
                LOG.debug("Bulk Write to MongoDB failed, retry times = {}", i, e);
                if (i >= maxRetries) {
                    LOG.error("Bulk Write to MongoDB failed", e);
                    throw new IOException(e);
                }
                sleepBeforeRetry(retryIntervalMs, i, e);
            }
        }
    }

    private void executeBulkWrite() {
        mongoClient
                .getDatabase(connectionOptions.getDatabase())
                .getCollection(connectionOptions.getCollection(), BsonDocument.class)
                .bulkWrite(
                        bulkRequests,
                        new BulkWriteOptions()
                                .ordered(writeOptions.isOrderedWrites())
                                .bypassDocumentValidation(
                                        writeOptions.isBypassDocumentValidation()));
    }

    /**
     * Handles a {@link MongoBulkWriteException} by inspecting individual write errors. Skips
     * duplicate key errors (code 11000) which are expected during AT_LEAST_ONCE replay after
     * recovery. For ordered writes, re-queues remaining operations after the error index.
     */
    private HandleResult handleBulkWriteException(MongoBulkWriteException e) {
        List<BulkWriteError> writeErrors = e.getWriteErrors();
        if (writeErrors.isEmpty()) {
            return HandleResult.NEEDS_RETRY;
        }

        boolean allDuplicateKey = true;
        int duplicateKeyCount = 0;
        Set<Integer> errorIndices = new HashSet<>();
        for (BulkWriteError error : writeErrors) {
            errorIndices.add(error.getIndex());
            if (error.getCode() == DUPLICATE_KEY_ERROR_CODE) {
                duplicateKeyCount++;
            } else {
                allDuplicateKey = false;
            }
        }

        boolean ordered = writeOptions.isOrderedWrites();

        if (allDuplicateKey) {
            return handleAllDuplicateKeys(errorIndices, ordered);
        }

        return handleMixedErrors(writeErrors, errorIndices, duplicateKeyCount, ordered);
    }

    private HandleResult handleAllDuplicateKeys(Set<Integer> errorIndices, boolean ordered) {
        int totalOps = bulkRequests.size();
        int duplicates = errorIndices.size();

        if (ordered) {
            // With ordered=true, MongoDB stops at the first error and reports only
            // that one error. So errorIndices always has exactly one element here.
            // Operations after the error index were never attempted and must be
            // re-queued.
            int maxErrorIndex = 0;
            for (int idx : errorIndices) {
                maxErrorIndex = Math.max(maxErrorIndex, idx);
            }
            int remainingOps = totalOps - maxErrorIndex - 1;

            if (remainingOps > 0) {
                List<WriteModel<BsonDocument>> remainingRequests =
                        new ArrayList<>(bulkRequests.subList(maxErrorIndex + 1, totalOps));
                LOG.info(
                        "Bulk write had {} duplicate key errors out of {} operations. "
                                + "{} operations never attempted (ordered=true). Re-queuing.",
                        duplicates,
                        totalOps,
                        remainingOps);
                bulkRequests.clear();
                bulkRequests.addAll(remainingRequests);
                return HandleResult.REMAINING_REQUEUED;
            }
        }

        // For unordered writes, MongoDB attempts ALL operations. Non-duplicate records
        // already succeeded in the same bulkWrite call. Nothing to re-queue.
        LOG.info(
                "Bulk write had {} duplicate key errors out of {} operations. "
                        + "All non-duplicate operations succeeded. Skipping duplicates.",
                duplicates,
                totalOps);
        bulkRequests.clear();
        return HandleResult.DUPLICATES_SKIPPED;
    }

    private HandleResult handleMixedErrors(
            List<BulkWriteError> writeErrors,
            Set<Integer> errorIndices,
            int duplicateKeyCount,
            boolean ordered) {
        int totalOps = bulkRequests.size();
        List<WriteModel<BsonDocument>> retryRequests = new ArrayList<>();

        // Keep non-duplicate failed operations for retry
        for (BulkWriteError error : writeErrors) {
            if (error.getCode() != DUPLICATE_KEY_ERROR_CODE && error.getIndex() < totalOps) {
                retryRequests.add(bulkRequests.get(error.getIndex()));
            }
        }

        if (ordered) {
            // Re-queue remaining operations (ordered=true stops at first error)
            int maxErrorIndex = 0;
            for (int idx : errorIndices) {
                maxErrorIndex = Math.max(maxErrorIndex, idx);
            }
            if (maxErrorIndex + 1 < totalOps) {
                retryRequests.addAll(bulkRequests.subList(maxErrorIndex + 1, totalOps));
            }
        }

        int nonDuplicateErrors = errorIndices.size() - duplicateKeyCount;
        LOG.info(
                "Bulk write had {} errors ({} duplicate key, {} other) out of {} operations. "
                        + "Retrying {} operations.",
                errorIndices.size(),
                duplicateKeyCount,
                nonDuplicateErrors,
                totalOps,
                retryRequests.size());

        bulkRequests.clear();
        bulkRequests.addAll(retryRequests);
        return HandleResult.NEEDS_RETRY;
    }

    private static boolean hasDuplicateKeyError(MongoBulkWriteException e) {
        for (BulkWriteError error : e.getWriteErrors()) {
            if (error.getCode() == DUPLICATE_KEY_ERROR_CODE) {
                return true;
            }
        }
        return false;
    }

    private void sleepBeforeRetry(long retryIntervalMs, int retryCount, MongoException cause)
            throws IOException {
        try {
            Thread.sleep(retryIntervalMs * (retryCount + 1));
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException(
                    "Unable to flush; interrupted while doing another attempt", cause);
        }
    }

    private boolean isOverMaxBatchSizeLimit() {
        return batchSize != -1 && bulkRequests.size() >= batchSize;
    }

    private boolean isOverMaxBatchIntervalLimit() {
        long lastSentInterval = System.currentTimeMillis() - lastSendTime;
        return batchIntervalMs != -1 && lastSentInterval >= batchIntervalMs;
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to MongoDB failed.", flushException);
        }
    }
}
