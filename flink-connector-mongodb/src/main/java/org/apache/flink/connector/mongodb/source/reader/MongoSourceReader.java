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

package org.apache.flink.connector.mongodb.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.mongodb.source.split.MongoScanSourceSplit;
import org.apache.flink.connector.mongodb.source.split.MongoScanSourceSplitState;
import org.apache.flink.connector.mongodb.source.split.MongoSourceSplit;
import org.apache.flink.connector.mongodb.source.split.MongoSourceSplitState;
import org.apache.flink.connector.mongodb.source.split.MongoStreamSourceSplit;
import org.apache.flink.connector.mongodb.source.split.MongoStreamSourceSplitState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * The common mongo source reader for both ordered & unordered message consuming.
 *
 * @param <OUT> The output message type for flink.
 */
@Internal
public class MongoSourceReader<OUT>
        extends SingleThreadMultiplexSourceReaderBase<
                MongoSourceRecord, OUT, MongoSourceSplit, MongoSourceSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoSourceReader.class);

    public MongoSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<MongoSourceRecord>> elementQueue,
            Supplier<SplitReader<MongoSourceRecord, MongoSourceSplit>> splitReaderSupplier,
            RecordEmitter<MongoSourceRecord, OUT, MongoSourceSplitState> recordEmitter,
            MongoSourceReaderContext readerContext) {
        super(
                elementQueue,
                new SingleThreadFetcherManager<>(elementQueue, splitReaderSupplier),
                recordEmitter,
                readerContext.getConfiguration(),
                readerContext);
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, MongoSourceSplitState> finishedSplitIds) {
        for (MongoSourceSplitState splitState : finishedSplitIds.values()) {
            MongoSourceSplit sourceSplit = splitState.toMongoSourceSplit();
            checkState(
                    sourceSplit instanceof MongoScanSourceSplit,
                    String.format(
                            "Only scan split could finish, but the actual split is stream split %s",
                            sourceSplit));
            LOG.info("Split {} is finished.", sourceSplit.splitId());
        }

        context.sendSplitRequest();
    }

    @Override
    protected MongoSourceSplitState initializedState(MongoSourceSplit split) {
        if (split instanceof MongoScanSourceSplit) {
            return new MongoScanSourceSplitState((MongoScanSourceSplit) split);
        } else if (split instanceof MongoStreamSourceSplit) {
            return new MongoStreamSourceSplitState((MongoStreamSourceSplit) split);
        } else {
            throw new IllegalStateException("Unknown split type " + split.getClass().getName());
        }
    }

    @Override
    protected MongoSourceSplit toSplitType(String splitId, MongoSourceSplitState splitState) {
        return splitState.toMongoSourceSplit();
    }
}
