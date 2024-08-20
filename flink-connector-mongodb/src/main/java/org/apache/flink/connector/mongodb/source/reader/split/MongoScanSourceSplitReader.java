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

package org.apache.flink.connector.mongodb.source.reader.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.mongodb.common.config.MongoConnectionOptions;
import org.apache.flink.connector.mongodb.source.config.MongoReadOptions;
import org.apache.flink.connector.mongodb.source.reader.MongoSourceReaderContext;
import org.apache.flink.connector.mongodb.source.split.MongoScanSourceSplit;
import org.apache.flink.connector.mongodb.source.split.MongoSourceSplit;
import org.apache.flink.util.CollectionUtil;

import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lt;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.HASHED_INDEX_TYPE;
import static org.apache.flink.connector.mongodb.common.utils.MongoUtils.project;

/** A split reader implements {@link SplitReader} for {@link MongoScanSourceSplit}. */
@Internal
public class MongoScanSourceSplitReader implements MongoSourceSplitReader<MongoSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoScanSourceSplitReader.class);

    private final MongoConnectionOptions connectionOptions;
    private final MongoReadOptions readOptions;
    private final MongoSourceReaderContext readerContext;
    @Nullable private final List<String> projectedFields;

    private final Bson filter;

    private boolean closed = false;
    private boolean finished = false;
    private MongoClient mongoClient;
    private MongoCursor<BsonDocument> currentCursor;
    private MongoScanSourceSplit currentSplit;

    public MongoScanSourceSplitReader(
            MongoConnectionOptions connectionOptions,
            MongoReadOptions readOptions,
            @Nullable List<String> projectedFields,
            Bson filter,
            MongoSourceReaderContext readerContext) {
        this.connectionOptions = connectionOptions;
        this.readOptions = readOptions;
        this.projectedFields = projectedFields;
        this.filter = filter;
        this.readerContext = readerContext;
    }

    @Override
    public RecordsWithSplitIds<BsonDocument> fetch() throws IOException {
        if (closed) {
            throw new IllegalStateException("Cannot fetch records from a closed split reader");
        }

        RecordsBySplits.Builder<BsonDocument> builder = new RecordsBySplits.Builder<>();

        // Return when no split registered to this reader.
        if (currentSplit == null) {
            return builder.build();
        }

        // Return when current read count is over limit.
        if (readerContext.isOverLimit()) {
            builder.addFinishedSplit(currentSplit.splitId());
            currentSplit = null;
            finished = true;
            return builder.build();
        }

        currentCursor = getOrCreateCursor();
        int fetchSize = readOptions.getFetchSize();

        try {
            for (int recordNum = 0; recordNum < fetchSize; recordNum++) {
                if (currentCursor.hasNext()) {
                    builder.add(currentSplit, currentCursor.next());
                    readerContext.getReadCount().incrementAndGet();
                    if (readerContext.isOverLimit()) {
                        builder.addFinishedSplit(currentSplit.splitId());
                        finished = true;
                        break;
                    }
                } else {
                    builder.addFinishedSplit(currentSplit.splitId());
                    finished = true;
                    break;
                }
            }
            return builder.build();
        } catch (MongoException e) {
            throw new IOException("Scan records form MongoDB failed", e);
        } finally {
            if (finished) {
                closeCursor();
            }
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<MongoSourceSplit> splitsChanges) {
        LOG.debug("Handle split changes {}", splitsChanges);

        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        MongoSourceSplit sourceSplit = splitsChanges.splits().get(0);
        if (!(sourceSplit instanceof MongoScanSourceSplit)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SourceSplit type of %s is not supported.",
                            sourceSplit.getClass()));
        }

        this.currentSplit = (MongoScanSourceSplit) sourceSplit;
        this.finished = false;
    }

    @Override
    public void wakeUp() {
        // Close current cursor to cancel blocked hasNext(), next().
        closeCursor();
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            closeCursor();
        }
    }

    private MongoCursor<BsonDocument> getOrCreateCursor() {
        if (currentCursor == null) {
            LOG.debug("Opened cursor for split: {}", currentSplit);
            mongoClient = MongoClients.create(connectionOptions.getUri());

            BsonDocument min = currentSplit.getMin();
            BsonDocument max = currentSplit.getMax();
            BsonDocument hint = currentSplit.getHint();
            boolean isSplitByCompoundOrHashIndexes =
                    hint.size() > 1 || hint.containsValue(HASHED_INDEX_TYPE);

            MongoCollection<BsonDocument> collection =
                    mongoClient
                            .getDatabase(connectionOptions.getDatabase())
                            .getCollection(connectionOptions.getCollection(), BsonDocument.class);

            FindIterable<BsonDocument> findIterable;
            if (isSplitByCompoundOrHashIndexes) {
                // Using MongoDB's cursor.min() and cursor.max() to limit an index bound.
                // When the index range is the primary key, the bound is (min <= _id < max).
                // Compound indexes and hash indexes bounds can be supported in this way.
                // Please refer to https://www.mongodb.com/docs/manual/reference/method/cursor.min/
                findIterable =
                        collection
                                .find(filter)
                                .min(min)
                                .max(max)
                                .hint(hint)
                                .noCursorTimeout(readOptions.isNoCursorTimeout());
            } else {
                // Using MongoDB's $gte and $lt filters to limit split bounds.
                // This method is suitable for most scenarios,
                // but it does not support compound indexes and hash indexes bounds.
                Bson filters = filter;
                for (String key : hint.keySet()) {
                    filters = and(filters, gte(key, min.get(key)), lt(key, max.get(key)));
                }
                findIterable = collection.find(filters);
            }

            // Current split was partially read and recovered from checkpoint
            if (currentSplit.getOffset() > 0) {
                findIterable.skip(currentSplit.getOffset());
            }

            // Push limit down
            if (readerContext.isLimitPushedDown()) {
                findIterable.limit(readerContext.getLimit());
            }

            // Push projection down
            if (!CollectionUtil.isNullOrEmpty(projectedFields)) {
                findIterable.projection(project(projectedFields));
            }

            currentCursor = findIterable.cursor();
        }
        return currentCursor;
    }

    private void closeCursor() {
        if (currentCursor != null) {
            LOG.debug("Closing cursor for split: {}", currentSplit);
            try {
                currentCursor.close();
            } finally {
                currentCursor = null;
                try {
                    mongoClient.close();
                } finally {
                    mongoClient = null;
                }
            }
        }
    }
}
