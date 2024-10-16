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

package org.apache.flink.connector.mongodb.source.enumerator.splitter;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.mongodb.source.config.MongoReadOptions;
import org.apache.flink.connector.mongodb.source.split.MongoScanSourceSplit;
import org.apache.flink.util.FlinkRuntimeException;

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import org.apache.commons.collections.CollectionUtils;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MAX_BOUNDARY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MIN_KEY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_HINT;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.SPLIT_KEYS_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoUtils.splitVector;

/**
 * SplitVector Partitioner
 *
 * <p>Uses the SplitVector command to generate chunks for a collection. eg. <code>
 * db.runCommand({splitVector:"inventory.products", keyPattern:{_id:1}, maxChunkSize:64})</code>
 *
 * <p>Requires splitVector privilege.
 */
@Internal
public class MongoSplitVectorSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(MongoSplitVectorSplitter.class);

    private MongoSplitVectorSplitter() {}

    public static Collection<MongoScanSourceSplit> split(MongoSplitContext splitContext) {
        if (splitContext.isSharded()) {
            throw new FlinkRuntimeException("splitVector does not apply to sharded collections.");
        }

        MongoClient mongoClient = splitContext.getMongoClient();
        MongoNamespace namespace = splitContext.getMongoNamespace();
        MongoReadOptions readOptions = splitContext.getReadOptions();

        MemorySize chunkSize = readOptions.getPartitionSize();
        // if partition size < 1mb, use 1 mb as chunk size.
        int maxChunkSizeMB = Math.max(chunkSize.getMebiBytes(), 1);

        BsonDocument keyPattern = new BsonDocument(ID_FIELD, new BsonInt32(1));

        BsonDocument splitResult;
        try {
            splitResult = splitVector(mongoClient, namespace, keyPattern, maxChunkSizeMB);
        } catch (MongoException e) {
            LOG.error("Execute splitVector command failed : {}", e.getMessage());
            throw new FlinkRuntimeException(e);
        }

        BsonArray splitKeys = splitResult.getArray(SPLIT_KEYS_FIELD);
        if (CollectionUtils.isEmpty(splitKeys)) {
            // documents size is less than chunk size, treat the entire collection as single chunk.
            return MongoSingleSplitter.split(splitContext);
        }

        // Complete right bound: (lastKey, maxKey)
        splitKeys.add(BSON_MAX_BOUNDARY);

        List<MongoScanSourceSplit> sourceSplits = new ArrayList<>(splitKeys.size());

        BsonValue lowerValue = BSON_MIN_KEY;
        for (int i = 0; i < splitKeys.size(); i++) {
            BsonValue splitKeyValue = splitKeys.get(i).asDocument().get(ID_FIELD);
            sourceSplits.add(
                    new MongoScanSourceSplit(
                            String.format("%s_%d", namespace, i),
                            namespace.getDatabaseName(),
                            namespace.getCollectionName(),
                            new BsonDocument(ID_FIELD, lowerValue),
                            new BsonDocument(ID_FIELD, splitKeyValue),
                            ID_HINT));
            lowerValue = splitKeyValue;
        }

        return sourceSplits;
    }
}
