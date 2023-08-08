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

package org.apache.flink.connector.mongodb.source.enumerator.assigner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.mongodb.common.config.MongoConnectionOptions;
import org.apache.flink.connector.mongodb.source.config.MongoReadOptions;
import org.apache.flink.connector.mongodb.source.enumerator.MongoSourceEnumState;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.MongoSplitters;
import org.apache.flink.connector.mongodb.source.split.MongoScanSourceSplit;
import org.apache.flink.connector.mongodb.source.split.MongoSourceSplit;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.connector.mongodb.common.utils.MongoUtils.clientFor;
import static org.apache.flink.util.Preconditions.checkState;

/** The split assigner for {@link MongoScanSourceSplit}. */
@Internal
public class MongoScanSplitAssigner implements MongoSplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(MongoScanSplitAssigner.class);

    private final MongoConnectionOptions connectionOptions;
    private final MongoReadOptions readOptions;

    private final LinkedList<String> remainingCollections;
    private final List<String> alreadyProcessedCollections;
    private final LinkedList<MongoScanSourceSplit> remainingScanSplits;
    private final Map<String, MongoScanSourceSplit> assignedScanSplits;

    private boolean initialized;
    private MongoClient mongoClient;

    public MongoScanSplitAssigner(
            MongoConnectionOptions connectionOptions,
            MongoReadOptions readOptions,
            MongoSourceEnumState sourceEnumState) {
        this.connectionOptions = connectionOptions;
        this.readOptions = readOptions;
        this.remainingCollections = new LinkedList<>(sourceEnumState.getRemainingCollections());
        this.alreadyProcessedCollections = sourceEnumState.getAlreadyProcessedCollections();
        this.remainingScanSplits = new LinkedList<>(sourceEnumState.getRemainingScanSplits());
        this.assignedScanSplits = sourceEnumState.getAssignedScanSplits();
        this.initialized = sourceEnumState.isInitialized();
    }

    @Override
    public void open() {
        LOG.info("Mongo scan split assigner is opening.");
        if (!initialized) {
            String collectionId =
                    String.format(
                            "%s.%s",
                            connectionOptions.getDatabase(), connectionOptions.getCollection());
            remainingCollections.add(collectionId);
            mongoClient = clientFor(connectionOptions);
            initialized = true;
        }
    }

    @Override
    public Optional<MongoSourceSplit> getNext() {
        if (!remainingScanSplits.isEmpty()) {
            // return remaining splits firstly
            MongoScanSourceSplit split = remainingScanSplits.poll();
            assignedScanSplits.put(split.splitId(), split);
            return Optional.of(split);
        } else {
            // it's turn for next collection
            String nextCollection = remainingCollections.poll();
            if (nextCollection != null) {
                // split the given collection into chunks (scan splits)
                Collection<MongoScanSourceSplit> splits =
                        MongoSplitters.split(
                                mongoClient, readOptions, new MongoNamespace(nextCollection));
                remainingScanSplits.addAll(splits);
                alreadyProcessedCollections.add(nextCollection);
                return getNext();
            } else {
                return Optional.empty();
            }
        }
    }

    @Override
    public void addSplitsBack(Collection<MongoSourceSplit> splits) {
        for (MongoSourceSplit split : splits) {
            if (split instanceof MongoScanSourceSplit) {
                remainingScanSplits.add((MongoScanSourceSplit) split);
                // we should remove the add-backed splits from the assigned list,
                // because they are failed
                assignedScanSplits.remove(split.splitId());
            } else {
                throw new IllegalArgumentException(
                        "Cannot add stream split back to scan split assigner.");
            }
        }
    }

    @Override
    public MongoSourceEnumState snapshotState(long checkpointId) {
        return new MongoSourceEnumState(
                remainingCollections,
                alreadyProcessedCollections,
                remainingScanSplits,
                assignedScanSplits,
                initialized,
                false);
    }

    @Override
    public boolean noMoreScanSplits() {
        checkState(initialized, "The noMoreSplits method was called but not initialized.");
        return remainingCollections.isEmpty() && remainingScanSplits.isEmpty();
    }

    @Override
    public void close() throws IOException {
        if (mongoClient != null) {
            mongoClient.close();
            LOG.info("Mongo scan split assigner is closed.");
        }
    }
}
