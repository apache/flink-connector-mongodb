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
import org.apache.flink.connector.mongodb.source.config.MongoStartupOptions;
import org.apache.flink.connector.mongodb.source.enumerator.MongoSourceEnumState;
import org.apache.flink.connector.mongodb.source.split.MongoSourceSplit;
import org.apache.flink.connector.mongodb.source.split.MongoStreamOffset;
import org.apache.flink.connector.mongodb.source.split.MongoStreamSourceSplit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.connector.mongodb.common.utils.MongoUtils.displayCurrentOffset;
import static org.apache.flink.connector.mongodb.source.split.MongoStreamSourceSplit.STREAM_SPLIT_ID;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The split assigner for {@link MongoStreamSourceSplit}. */
@Internal
public class MongoStreamSplitAssigner implements MongoSplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(MongoStreamSplitAssigner.class);

    private final MongoConnectionOptions connectionOptions;
    private final MongoStartupOptions startupOptions;

    private boolean isStreamSplitAssigned;
    private Map<String, String> startupOffset;

    public MongoStreamSplitAssigner(
            MongoConnectionOptions connectionOptions, MongoStartupOptions startupOptions) {
        this.connectionOptions = connectionOptions;
        this.startupOptions = startupOptions;
    }

    @Override
    public void open() {
        switch (startupOptions.getStartupMode()) {
            case LATEST_OFFSET:
                this.startupOffset = displayCurrentOffset(connectionOptions).getOffset();
                break;
            case TIMESTAMP:
                this.startupOffset =
                        MongoStreamOffset.fromTimeMillis(
                                        checkNotNull(startupOptions.getStartupTimestampMillis()))
                                .getOffset();
                break;
            default:
                throw new IllegalStateException(
                        "Unsupported startup mode " + startupOptions.getStartupMode());
        }
    }

    @Override
    public void close() throws IOException {}

    @Override
    public Optional<MongoSourceSplit> getNext() {
        if (isStreamSplitAssigned) {
            return Optional.empty();
        } else {
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
    }

    @Override
    public void addSplitsBack(Collection<MongoSourceSplit> splits) {
        // will re-create stream split later
        isStreamSplitAssigned = false;
    }

    @Override
    public MongoSourceEnumState snapshotState(long checkpointId) {
        return MongoSourceEnumState.streamState(isStreamSplitAssigned);
    }

    @Override
    public boolean noMoreScanSplits() {
        return true;
    }
}
