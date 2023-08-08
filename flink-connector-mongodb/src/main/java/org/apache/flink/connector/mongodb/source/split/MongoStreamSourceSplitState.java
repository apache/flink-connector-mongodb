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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.mongodb.source.reader.MongoSourceRecord;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_FIELD;

/** MongoDB source split state for {@link MongoStreamSourceSplit}. */
@Internal
public class MongoStreamSourceSplitState implements MongoSourceSplitState {

    private final MongoStreamSourceSplit streamSplit;

    private MongoStreamOffset offset;

    public MongoStreamSourceSplitState(MongoStreamSourceSplit streamSplit) {
        this.streamSplit = streamSplit;
        this.offset = streamSplit.streamOffset();
    }

    @Override
    public MongoStreamSourceSplit toMongoSourceSplit() {
        return new MongoStreamSourceSplit(
                streamSplit.splitId(),
                streamSplit.getDatabase(),
                streamSplit.getCollection(),
                offset);
    }

    @Override
    public void updateOffset(MongoSourceRecord record) {
        switch (record.getType()) {
            case HEARTBEAT:
                this.offset = MongoStreamOffset.fromHeartbeat(record.getRecord());
                break;
            case STREAM:
                this.offset =
                        MongoStreamOffset.fromResumeToken(record.getRecord().getDocument(ID_FIELD));
                break;
            default:
                throw new IllegalArgumentException(
                        "Expected stream or heartbeat record, but " + record.getType());
        }
    }
}
