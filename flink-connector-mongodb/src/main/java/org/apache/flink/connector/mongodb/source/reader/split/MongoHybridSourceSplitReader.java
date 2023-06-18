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
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.mongodb.common.config.MongoConnectionOptions;
import org.apache.flink.connector.mongodb.source.config.MongoChangeStreamOptions;
import org.apache.flink.connector.mongodb.source.config.MongoReadOptions;
import org.apache.flink.connector.mongodb.source.reader.MongoSourceReaderContext;
import org.apache.flink.connector.mongodb.source.reader.MongoSourceRecord;
import org.apache.flink.connector.mongodb.source.split.MongoScanSourceSplit;
import org.apache.flink.connector.mongodb.source.split.MongoSourceSplit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/** A split reader implements {@link SplitReader} for {@link MongoSourceSplit}. */
@Internal
public class MongoHybridSourceSplitReader implements MongoSourceSplitReader {

    private static final Logger LOG = LoggerFactory.getLogger(MongoHybridSourceSplitReader.class);

    private final MongoConnectionOptions connectionOptions;
    private final MongoReadOptions readOptions;
    private final MongoChangeStreamOptions changeStreamOptions;
    private final MongoSourceReaderContext readerContext;
    @Nullable private final List<String> projectedFields;
    @Nullable private MongoSourceSplitReader currentReader;

    public MongoHybridSourceSplitReader(
            MongoConnectionOptions connectionOptions,
            MongoReadOptions readOptions,
            MongoChangeStreamOptions changeStreamOptions,
            MongoSourceReaderContext readerContext,
            @Nullable List<String> projectedFields) {
        this.connectionOptions = connectionOptions;
        this.readOptions = readOptions;
        this.changeStreamOptions = changeStreamOptions;
        this.readerContext = readerContext;
        this.projectedFields = projectedFields;
    }

    @Override
    public RecordsWithSplitIds<MongoSourceRecord> fetch() throws IOException {
        if (currentReader == null) {
            throw new IOException("Split reader is not ready.");
        }
        return currentReader.fetch();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<MongoSourceSplit> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        switchReaderWhenNeeded(splitsChanges);
    }

    private void switchReaderWhenNeeded(SplitsChange<MongoSourceSplit> splitsChanges) {
        // the stream reader should keep alive
        if (currentReader instanceof MongoStreamSourceSplitReader) {
            return;
        }

        if (canAcceptNextSplit()) {
            MongoSourceSplit sourceSplit = splitsChanges.splits().get(0);
            if (sourceSplit instanceof MongoScanSourceSplit) {
                if (currentReader == null) {
                    currentReader =
                            new MongoScanSourceSplitReader(
                                    connectionOptions, readOptions, projectedFields, readerContext);
                }
            } else {
                // switch to stream reader
                if (currentReader != null) {
                    try {
                        currentReader.close();
                    } catch (Exception e) {
                        // ignore the exception
                        LOG.warn("Cannot close reader gracefully", e);
                    }
                }
                currentReader =
                        new MongoStreamSourceSplitReader(
                                connectionOptions, changeStreamOptions, readerContext);
            }
            currentReader.handleSplitsChanges(splitsChanges);
        }
    }

    @Override
    public void wakeUp() {
        if (currentReader != null) {
            LOG.info("Wakeup current reader {}", currentReader.getClass().getName());
            currentReader.wakeUp();
        }
    }

    @Override
    public void close() throws Exception {
        if (currentReader != null) {
            LOG.info("Close current reader {}", currentReader.getClass().getName());
            currentReader.close();
            currentReader = null;
        }
    }

    @Override
    public boolean canAcceptNextSplit() {
        return currentReader == null || currentReader.canAcceptNextSplit();
    }
}
