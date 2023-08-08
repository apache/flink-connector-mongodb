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

package org.apache.flink.connector.mongodb.common.utils;

import org.apache.flink.annotation.Internal;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonValue;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

/** Constants for MongoDB. */
@Internal
public class MongoConstants {

    public static final String ID_FIELD = "_id";

    public static final String ENCODE_VALUE_FIELD = "_value";

    public static final String NAMESPACE_FIELD = "ns";

    public static final String KEY_FIELD = "key";

    public static final String MAX_FIELD = "max";

    public static final String MIN_FIELD = "min";

    public static final String UUID_FIELD = "uuid";

    public static final String SPLIT_KEYS_FIELD = "splitKeys";

    public static final String SHARD_FIELD = "shard";

    public static final String SHARDED_FIELD = "sharded";

    public static final String COUNT_FIELD = "count";

    public static final String SIZE_FIELD = "size";

    public static final String AVG_OBJ_SIZE_FIELD = "avgObjSize";

    public static final String DROPPED_FIELD = "dropped";

    public static final String CLUSTER_TIME_FIELD = "clusterTime";

    public static final String RESUME_TOKEN_FIELD = "resumeToken";

    public static final String OPERATION_TYPE_FIELD = "operationType";

    public static final String DOCUMENT_KEY_FIELD = "documentKey";

    public static final String FULL_DOCUMENT_FIELD = "fullDocument";

    public static final String FULL_DOCUMENT_BEFORE_CHANGE_FIELD = "fullDocumentBeforeChange";

    public static final BsonValue BSON_MIN_KEY = new BsonMinKey();

    public static final BsonValue BSON_MAX_KEY = new BsonMaxKey();

    public static final BsonDocument ID_HINT = new BsonDocument(ID_FIELD, new BsonInt32(1));

    public static final JsonWriterSettings DEFAULT_JSON_WRITER_SETTINGS =
            JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();

    public static final int FAILED_TO_PARSE_ERROR = 9;

    public static final int UNAUTHORIZED_ERROR = 13;

    public static final int ILLEGAL_OPERATION_ERROR = 20;

    public static final int UNKNOWN_FIELD_ERROR = 40415;

    private MongoConstants() {}
}
