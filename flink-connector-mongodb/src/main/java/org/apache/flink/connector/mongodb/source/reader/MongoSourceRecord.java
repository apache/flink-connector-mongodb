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

package org.apache.flink.connector.mongodb.source.reader;

import org.apache.flink.annotation.PublicEvolving;

import org.bson.BsonDocument;
import org.bson.BsonString;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.DEFAULT_JSON_WRITER_SETTINGS;

/** Source record produced by {@link MongoSourceReader}. * */
@PublicEvolving
public class MongoSourceRecord {

    public static final String TYPE_FIELD = "type";
    public static final String RECORD_FIELD = "record";

    private final RecordType type;
    private final BsonDocument record;

    private MongoSourceRecord(RecordType type, BsonDocument record) {
        this.type = type;
        this.record = record;
    }

    public static MongoSourceRecord snapshotRecord(BsonDocument record) {
        return new MongoSourceRecord(RecordType.SNAPSHOT, record);
    }

    public static MongoSourceRecord streamRecord(BsonDocument record) {
        return new MongoSourceRecord(RecordType.STREAM, record);
    }

    public static MongoSourceRecord heartbeatRecord(BsonDocument record) {
        return new MongoSourceRecord(RecordType.HEARTBEAT, record);
    }

    public RecordType getType() {
        return type;
    }

    public BsonDocument getRecord() {
        return record;
    }

    public String toJson() {
        return new BsonDocument(TYPE_FIELD, new BsonString(type.name()))
                .append(RECORD_FIELD, record)
                .toJson(DEFAULT_JSON_WRITER_SETTINGS);
    }

    /** Enum to describe record type for {@link MongoSourceRecord}. */
    @PublicEvolving
    public enum RecordType {
        SNAPSHOT,
        STREAM,
        HEARTBEAT
    }
}
