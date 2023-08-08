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

package org.apache.flink.connector.mongodb.source.split;

import org.apache.flink.connector.mongodb.common.utils.MongoConstants;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_HINT;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link MongoSourceSplitSerializer}. */
class MongoSourceSplitSerializerTest {

    @Test
    void serializeAndDeserializeMongoScanSourceSplit() throws IOException {
        MongoScanSourceSplit scanSourceSplit = createScanSourceSplit();
        byte[] bytes = MongoSourceSplitSerializer.INSTANCE.serialize(scanSourceSplit);
        MongoSourceSplit deserialized = MongoSourceSplitSerializer.INSTANCE.deserialize(1, bytes);
        assertThat(deserialized).isEqualTo(scanSourceSplit);
    }

    @Test
    void serializeAndDeserializeMongoStreamSourceSplit() throws IOException {
        MongoStreamSourceSplit streamSourceSplit = createStreamSourceSplit();
        byte[] bytes = MongoSourceSplitSerializer.INSTANCE.serialize(streamSourceSplit);
        MongoSourceSplit deserialized = MongoSourceSplitSerializer.INSTANCE.deserialize(1, bytes);
        assertThat(deserialized).isEqualTo(streamSourceSplit);
    }

    private static MongoScanSourceSplit createScanSourceSplit() {
        return new MongoScanSourceSplit(
                "split",
                "db",
                "coll",
                new BsonDocument("_id", new BsonInt32(0)),
                new BsonDocument("_id", MongoConstants.BSON_MAX_KEY),
                ID_HINT);
    }

    private static MongoStreamSourceSplit createStreamSourceSplit() {
        return new MongoStreamSourceSplit(
                "split",
                "db",
                "coll",
                MongoStreamOffset.fromTimeMillis(System.currentTimeMillis()));
    }
}
