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

package org.apache.flink.connector.mongodb.source.reader.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.mongodb.common.config.MongoConnectionOptions;
import org.apache.flink.connector.mongodb.source.config.MongoChangeStreamOptions;
import org.apache.flink.connector.mongodb.source.reader.MongoSourceReaderContext;
import org.apache.flink.connector.mongodb.source.reader.MongoSourceRecord;
import org.apache.flink.connector.mongodb.source.split.MongoSourceSplit;
import org.apache.flink.connector.mongodb.source.split.MongoStreamSourceSplit;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;

import com.mongodb.MongoCommandException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Optional;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.CLUSTER_TIME_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.FAILED_TO_PARSE_ERROR;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ILLEGAL_OPERATION_ERROR;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.RESUME_TOKEN_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.UNAUTHORIZED_ERROR;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.UNKNOWN_FIELD_ERROR;
import static org.apache.flink.connector.mongodb.common.utils.MongoUtils.clientFor;
import static org.apache.flink.connector.mongodb.common.utils.MongoUtils.currentClusterTime;
import static org.apache.flink.connector.mongodb.common.utils.MongoUtils.getChangeStreamIterable;
import static org.apache.flink.connector.mongodb.source.reader.MongoSourceRecord.heartbeatRecord;
import static org.apache.flink.connector.mongodb.source.reader.MongoSourceRecord.streamRecord;

/** A split reader implements {@link SplitReader} for {@link MongoStreamSourceSplit}. */
@Internal
public class MongoStreamSourceSplitReader implements MongoSourceSplitReader {

    private static final Logger LOG = LoggerFactory.getLogger(MongoScanSourceSplitReader.class);

    private final MongoConnectionOptions connectionOptions;
    private final MongoChangeStreamOptions changeStreamOptions;
    private final MongoSourceReaderContext readerContext;

    private boolean closed = false;
    private MongoClient mongoClient;
    private MongoChangeStreamCursor<RawBsonDocument> currentCursor;
    private MongoStreamSourceSplit currentSplit;

    private boolean supportsStartAtOperationTime = true;
    private boolean supportsStartAfter = true;
    private boolean supportsFullDocumentBeforeChange = true;

    public MongoStreamSourceSplitReader(
            MongoConnectionOptions connectionOptions,
            MongoChangeStreamOptions changeStreamOptions,
            MongoSourceReaderContext readerContext) {
        this.connectionOptions = connectionOptions;
        this.changeStreamOptions = changeStreamOptions;
        this.readerContext = readerContext;
    }

    @Override
    public RecordsWithSplitIds<MongoSourceRecord> fetch() throws IOException {
        if (closed) {
            throw new IOException("Cannot fetch records from a closed split reader");
        }

        RecordsBySplits.Builder<MongoSourceRecord> builder = new RecordsBySplits.Builder<>();

        // Return when no split registered to this reader.
        if (currentSplit == null) {
            return builder.build();
        }

        if (currentCursor == null) {
            currentCursor = openChangeStreamCursor();
        }

        try {
            for (int recordNum = 0; recordNum < changeStreamOptions.getFetchSize(); recordNum++) {
                BsonDocument nextRecord = currentCursor.tryNext();
                if (nextRecord != null) {
                    builder.add(currentSplit, streamRecord(nextRecord));
                } else {
                    builder.add(currentSplit, heartbeatRecord(heartbeat()));
                    break;
                }
            }
            return builder.build();
        } catch (Exception e) {
            throw new IOException("Poll change stream records failed", e);
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<MongoSourceSplit> splitsChanges) {
        LOG.debug("Handle split changes {}", splitsChanges);

        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass().getName()));
        }

        MongoSourceSplit sourceSplit = splitsChanges.splits().get(0);
        if (!(sourceSplit instanceof MongoStreamSourceSplit)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SourceSplit type of %s is not supported.",
                            sourceSplit.getClass().getName()));
        }

        if (canAcceptNextSplit()) {
            this.currentSplit = (MongoStreamSourceSplit) sourceSplit;
        }
    }

    @Override
    public void wakeUp() {
        // Close current cursor to cancel blocked hasNext(), next().
        closeCursor();
    }

    @Override
    public void close() throws Exception {
        if (!closed) {
            closed = true;
            closeCursor();
        }
    }

    private MongoChangeStreamCursor<RawBsonDocument> openChangeStreamCursor() {
        LOG.debug("Opened change stream cursor for partitionId: {}", currentSplit);
        mongoClient = clientFor(connectionOptions);

        ChangeStreamIterable<?> changeStreamIterable =
                getChangeStreamIterable(
                        mongoClient, currentSplit.getDatabase(), currentSplit.getCollection());

        Optional<BsonDocument> resumeToken = currentSplit.streamOffset().getResumeToken();
        Optional<BsonTimestamp> operationTime = currentSplit.streamOffset().getClusterTime();

        if (resumeToken.isPresent()) {
            if (supportsStartAfter) {
                LOG.info("Open the change stream after the previous offset: {}", resumeToken.get());
                changeStreamIterable.startAfter(resumeToken.get());
            } else {
                LOG.info(
                        "Open the change stream after the previous offset using resumeAfter: {}",
                        resumeToken.get());
                changeStreamIterable.resumeAfter(resumeToken.get());
            }
        } else {
            if (supportsStartAtOperationTime) {
                changeStreamIterable.startAtOperationTime(
                        operationTime.orElseThrow(NoSuchElementException::new));
                LOG.info("Open the change stream at the operationTime: {}", operationTime.get());
            } else {
                LOG.warn("Open the change stream of the latest offset");
            }
        }

        changeStreamIterable.fullDocument(changeStreamOptions.getFullDocument());
        if (supportsFullDocumentBeforeChange) {
            changeStreamIterable.fullDocumentBeforeChange(
                    changeStreamOptions.getFullDocumentBeforeChange());
        }

        try {
            return (MongoChangeStreamCursor<RawBsonDocument>)
                    changeStreamIterable.withDocumentClass(RawBsonDocument.class).cursor();
        } catch (MongoCommandException e) {
            if (e.getErrorCode() == FAILED_TO_PARSE_ERROR
                    || e.getErrorCode() == UNKNOWN_FIELD_ERROR) {
                if (e.getErrorMessage().contains("startAtOperationTime")) {
                    supportsStartAtOperationTime = false;
                    return openChangeStreamCursor();
                } else if (e.getErrorMessage().contains("startAfter")) {
                    supportsStartAfter = false;
                    return openChangeStreamCursor();
                } else if (e.getErrorMessage().contains("fullDocumentBeforeChange")) {
                    supportsFullDocumentBeforeChange = false;
                    return openChangeStreamCursor();
                } else {
                    LOG.error("Open change stream failed ", e);
                    throw new FlinkRuntimeException("Open change stream failed", e);
                }
            } else if (e.getErrorCode() == ILLEGAL_OPERATION_ERROR) {
                LOG.error(
                        "Illegal $changeStream operation: {} {}",
                        e.getErrorMessage(),
                        e.getErrorCode());
                throw new FlinkRuntimeException("Illegal $changeStream operation", e);
            } else if (e.getErrorCode() == UNAUTHORIZED_ERROR) {
                LOG.error(
                        "Unauthorized $changeStream operation: {} {}",
                        e.getErrorMessage(),
                        e.getErrorCode());
                throw new FlinkRuntimeException("Unauthorized $changeStream operation", e);
            } else {
                LOG.error("Open change stream failed ", e);
                throw new FlinkRuntimeException("Open change stream failed", e);
            }
        }
    }

    private void closeCursor() {
        LOG.debug("Closing change stream cursor for split: {}", currentSplit);
        IOUtils.closeAllQuietly(currentCursor, mongoClient);
        currentCursor = null;
        mongoClient = null;
    }

    private BsonDocument heartbeat() {
        if (currentCursor.getResumeToken() != null) {
            return new BsonDocument(RESUME_TOKEN_FIELD, currentCursor.getResumeToken());
        } else {
            return new BsonDocument(CLUSTER_TIME_FIELD, currentClusterTime(mongoClient));
        }
    }

    @Override
    public boolean canAcceptNextSplit() {
        return currentSplit == null;
    }
}
