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

import org.apache.flink.annotation.PublicEvolving;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.CLUSTER_TIME_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.DEFAULT_JSON_WRITER_SETTINGS;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.RESUME_TOKEN_FIELD;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The MongoDB change stream offset, record resumeToken and clusterTime. */
@PublicEvolving
public class MongoStreamOffset {

    private Map<String, String> offset;

    private MongoStreamOffset(BsonDocument resumeToken) {
        Map<String, String> offset = new HashMap<>();
        offset.put(RESUME_TOKEN_FIELD, resumeToken.toJson(DEFAULT_JSON_WRITER_SETTINGS));
        this.offset = offset;
    }

    private MongoStreamOffset(BsonTimestamp clusterTime) {
        Map<String, String> offset = new HashMap<>();
        offset.put(CLUSTER_TIME_FIELD, String.valueOf(clusterTime.getValue()));
        this.offset = offset;
    }

    private MongoStreamOffset(Map<String, String> offset) {
        this.offset = checkNotNull(offset);
    }

    public static MongoStreamOffset fromResumeToken(BsonDocument resumeToken) {
        return new MongoStreamOffset(resumeToken);
    }

    public static MongoStreamOffset fromClusterTime(BsonTimestamp clusterTime) {
        return new MongoStreamOffset(clusterTime);
    }

    public static MongoStreamOffset fromHeartbeat(BsonDocument heartbeat) {
        if (heartbeat.containsKey(RESUME_TOKEN_FIELD)) {
            return fromResumeToken(heartbeat.getDocument(RESUME_TOKEN_FIELD));
        } else {
            return fromClusterTime(heartbeat.getTimestamp(CLUSTER_TIME_FIELD));
        }
    }

    public static MongoStreamOffset fromTimeMillis(long timeMillis) {
        int seconds = (int) TimeUnit.MILLISECONDS.toSeconds(timeMillis);
        return new MongoStreamOffset(new BsonTimestamp(seconds, 0));
    }

    public static MongoStreamOffset fromOffset(Map<String, String> offset) {
        return new MongoStreamOffset(offset);
    }

    public Optional<BsonDocument> getResumeToken() {
        return Optional.ofNullable(offset.get(RESUME_TOKEN_FIELD)).map(BsonDocument::parse);
    }

    public Optional<BsonTimestamp> getClusterTime() {
        return Optional.ofNullable(offset.get(CLUSTER_TIME_FIELD))
                .map(Long::valueOf)
                .map(BsonTimestamp::new);
    }

    public Map<String, String> getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return offset.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoStreamOffset that = (MongoStreamOffset) o;
        return Objects.equals(offset, that.offset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset);
    }
}
