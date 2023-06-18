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

package org.apache.flink.connector.mongodb.source.enumerator.assigner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.mongodb.common.config.MongoConnectionOptions;
import org.apache.flink.connector.mongodb.source.config.MongoReadOptions;
import org.apache.flink.connector.mongodb.source.enumerator.MongoSourceEnumState;
import org.apache.flink.connector.mongodb.source.split.MongoScanSourceSplit;
import org.apache.flink.connector.mongodb.source.split.MongoSourceSplit;
import org.apache.flink.connector.mongodb.source.split.MongoStreamOffset;
import org.apache.flink.connector.mongodb.source.split.MongoStreamSourceSplit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.connector.mongodb.common.utils.MongoUtils.displayCurrentOffset;
import static org.apache.flink.connector.mongodb.source.split.MongoStreamSourceSplit.STREAM_SPLIT_ID;

/**
 * The hybrid split assigner for both {@link MongoScanSourceSplit} and {@link
 * MongoStreamSourceSplit}.
 */
@Internal
public class MongoHybridSplitAssigner implements MongoSplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(MongoHybridSplitAssigner.class);

    private final MongoConnectionOptions connectionOptions;
    private final MongoScanSplitAssigner scanSplitAssigner;

    private boolean isStreamSplitAssigned;
    private Map<String, String> startupOffset;

    public MongoHybridSplitAssigner(
            MongoConnectionOptions connectionOptions,
            MongoReadOptions readOptions,
            MongoSourceEnumState sourceEnumState) {
        this.connectionOptions = connectionOptions;
        this.isStreamSplitAssigned = sourceEnumState.isStreamSplitAssigned();
        this.scanSplitAssigner =
                new MongoScanSplitAssigner(connectionOptions, readOptions, sourceEnumState);
    }

    @Override
    public Optional<MongoSourceSplit> getNext() {
        if (scanSplitAssigner.noMoreScanSplits()) {
            // stream split assigning
            if (isStreamSplitAssigned) {
                // no more splits for the assigner
                return Optional.empty();
            } else {
                // assigning the stream split.
                isStreamSplitAssigned = true;
                MongoStreamSourceSplit streamSplit =
                        new MongoStreamSourceSplit(
                                STREAM_SPLIT_ID,
                                connectionOptions.getDatabase(),
                                connectionOptions.getCollection(),
                                MongoStreamOffset.fromOffset(startupOffset));
                LOG.info("Mongo stream split assigned {}", streamSplit);
                return Optional.of(streamSplit);
            }
        } else {
            // scan assigner still have remaining splits, assign split from it
            return scanSplitAssigner.getNext();
        }
    }

    @Override
    public void open() {
        startupOffset = displayCurrentOffset(connectionOptions).getOffset();
        LOG.info("Initialized startup offset {}", startupOffset);
        scanSplitAssigner.open();
    }

    @Override
    public void close() throws IOException {
        scanSplitAssigner.close();
    }

    @Override
    public void addSplitsBack(Collection<MongoSourceSplit> splits) {
        List<MongoSourceSplit> scanSplits = new ArrayList<>();
        for (MongoSourceSplit split : splits) {
            if (split instanceof MongoScanSourceSplit) {
                scanSplits.add(split);
            } else if (split instanceof MongoStreamSourceSplit) {
                MongoStreamSourceSplit streamSplit = (MongoStreamSourceSplit) split;
                startupOffset = streamSplit.streamOffset().getOffset();
                isStreamSplitAssigned = false;
            } else {
                throw new IllegalStateException(
                        "Unsupported mongo split type " + split.getClass().getName());
            }
        }

        if (!scanSplits.isEmpty()) {
            scanSplitAssigner.addSplitsBack(scanSplits);
        }
    }

    @Override
    public MongoSourceEnumState snapshotState(long checkpointId) {
        return MongoSourceEnumState.hybridState(
                scanSplitAssigner.snapshotState(checkpointId), isStreamSplitAssigned);
    }

    @Override
    public boolean noMoreScanSplits() {
        return scanSplitAssigner.noMoreScanSplits();
    }
}
