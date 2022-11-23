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

import org.apache.flink.connector.mongodb.common.utils.MongoConstants;
import org.apache.flink.connector.mongodb.source.split.MongoScanSourceSplit;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_HINT;
import static org.apache.flink.connector.mongodb.source.enumerator.MongoSourceEnumStateSerializer.INSTANCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

/** Unit tests for {@link MongoSourceEnumStateSerializer}. */
public class MongoSourceEnumStateSerializerTest {

    @Test
    void serializeAndDeserializeMongoSourceEnumState() throws Exception {
        boolean initialized = false;
        List<String> remainingCollections = Arrays.asList("db.remains0", "db.remains1");
        List<String> alreadyProcessedCollections = Arrays.asList("db.processed0", "db.processed1");
        List<MongoScanSourceSplit> remainingScanSplits =
                Arrays.asList(createSourceSplit(0), createSourceSplit(1));

        Map<String, MongoScanSourceSplit> assignedScanSplits =
                Collections.singletonMap("split2", createSourceSplit(2));

        MongoSourceEnumState state =
                new MongoSourceEnumState(
                        remainingCollections,
                        alreadyProcessedCollections,
                        remainingScanSplits,
                        assignedScanSplits,
                        initialized);

        byte[] bytes = INSTANCE.serialize(state);
        MongoSourceEnumState state1 = INSTANCE.deserialize(INSTANCE.getVersion(), bytes);

        assertEquals(state.getRemainingCollections(), state1.getRemainingCollections());
        assertEquals(
                state.getAlreadyProcessedCollections(), state1.getAlreadyProcessedCollections());
        assertEquals(state.getRemainingScanSplits(), state1.getRemainingScanSplits());
        assertEquals(state.getAssignedScanSplits(), state1.getAssignedScanSplits());
        assertEquals(state.isInitialized(), state1.isInitialized());

        assertNotSame(state, state1);
    }

    private static MongoScanSourceSplit createSourceSplit(int index) {
        return new MongoScanSourceSplit(
                "split" + index,
                "db",
                "coll",
                new BsonDocument("_id", new BsonInt32(index)),
                new BsonDocument("_id", MongoConstants.BSON_MAX_KEY),
                ID_HINT);
    }
}
