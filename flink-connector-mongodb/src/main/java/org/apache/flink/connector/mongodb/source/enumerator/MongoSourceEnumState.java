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

package org.apache.flink.connector.mongodb.source.enumerator;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.mongodb.source.enumerator.assigner.MongoSplitAssigner;
import org.apache.flink.connector.mongodb.source.reader.split.MongoHybridSourceSplitReader;
import org.apache.flink.connector.mongodb.source.split.MongoScanSourceSplit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The state class for MongoDB source enumerator, used for storing the split state. This class is
 * managed and controlled by {@link MongoSplitAssigner}.
 */
@PublicEvolving
public class MongoSourceEnumState {

    /** The Mongo collections remaining. */
    private final List<String> remainingCollections;

    /**
     * The paths that are no longer in the enumerator checkpoint, but have been processed before.
     */
    private final List<String> alreadyProcessedCollections;

    /** The scan splits in the checkpoint. */
    private final List<MongoScanSourceSplit> remainingScanSplits;

    /**
     * The scan splits that the {@link MongoSourceEnumerator} has assigned to {@link
     * MongoHybridSourceSplitReader}s.
     */
    private final Map<String, MongoScanSourceSplit> assignedScanSplits;

    /** The pipeline has been triggered and collection partitions have been assigned to readers. */
    private final boolean initialized;

    /** Whether the stream split has been assigned. */
    private final boolean streamSplitAssigned;

    public MongoSourceEnumState(
            List<String> remainingCollections,
            List<String> alreadyProcessedCollections,
            List<MongoScanSourceSplit> remainingScanSplits,
            Map<String, MongoScanSourceSplit> assignedScanSplits,
            boolean initialized,
            boolean streamSplitAssigned) {
        this.remainingCollections = remainingCollections;
        this.alreadyProcessedCollections = alreadyProcessedCollections;
        this.remainingScanSplits = remainingScanSplits;
        this.assignedScanSplits = assignedScanSplits;
        this.initialized = initialized;
        this.streamSplitAssigned = streamSplitAssigned;
    }

    public List<String> getRemainingCollections() {
        return remainingCollections;
    }

    public List<String> getAlreadyProcessedCollections() {
        return alreadyProcessedCollections;
    }

    public List<MongoScanSourceSplit> getRemainingScanSplits() {
        return remainingScanSplits;
    }

    public Map<String, MongoScanSourceSplit> getAssignedScanSplits() {
        return assignedScanSplits;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public boolean isStreamSplitAssigned() {
        return streamSplitAssigned;
    }

    /** The initial assignment state for Mongo. */
    public static MongoSourceEnumState initialState() {
        return new MongoSourceEnumState(
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new HashMap<>(),
                false,
                false);
    }

    /** The stream enum state for Mongo. */
    public static MongoSourceEnumState streamState(boolean streamSplitAssigned) {
        return new MongoSourceEnumState(
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new HashMap<>(),
                true,
                streamSplitAssigned);
    }

    /** The hybrid enum state for Mongo. */
    public static MongoSourceEnumState hybridState(
            MongoSourceEnumState scanState, boolean streamSplitAssigned) {
        return new MongoSourceEnumState(
                scanState.remainingCollections,
                scanState.alreadyProcessedCollections,
                scanState.remainingScanSplits,
                scanState.assignedScanSplits,
                scanState.initialized,
                streamSplitAssigned);
    }
}
