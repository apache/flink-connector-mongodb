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
import org.apache.flink.connector.mongodb.source.split.MongoScanSourceSplit;
import org.apache.flink.util.FlinkRuntimeException;

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.KEY_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.MAX_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.MIN_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoUtils.isShardedCollectionDropped;
import static org.apache.flink.connector.mongodb.common.utils.MongoUtils.readChunks;
import static org.apache.flink.connector.mongodb.common.utils.MongoUtils.readCollectionMetadata;

/**
 * Sharded Partitioner
 *
 * <p>Uses the chunks collection and partitions the collection based on the sharded collections
 * chunk ranges.
 *
 * <p>The following config collections' read privilege is required.
 *
 * <ul>
 *   <li>config.collections
 *   <li>config.chunks
 * </ul>
 */
@Internal
public class MongoShardedSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(MongoShardedSplitter.class);

    public static final MongoShardedSplitter INSTANCE = new MongoShardedSplitter();

    private MongoShardedSplitter() {}

    public Collection<MongoScanSourceSplit> split(MongoSplitContext splitContext) {
        MongoNamespace namespace = splitContext.getMongoNamespace();
        MongoClient mongoClient = splitContext.getMongoClient();

        List<BsonDocument> chunks;
        Optional<BsonDocument> collectionMetadata;
        try {
            collectionMetadata = readCollectionMetadata(mongoClient, namespace);
            if (!collectionMetadata.isPresent()) {
                LOG.error(
                        "Do sharded split failed, collection {} does not appear to be sharded.",
                        namespace);
                throw new FlinkRuntimeException(
                        String.format(
                                "Do sharded split failed, %s is not a sharded collection.",
                                namespace));
            }

            if (isShardedCollectionDropped(collectionMetadata.get())) {
                LOG.error("Do sharded split failed, collection {} was dropped.", namespace);
                throw new FlinkRuntimeException(
                        String.format("Do sharded split failed, %s was dropped.", namespace));
            }

            chunks = readChunks(mongoClient, collectionMetadata.get());
            if (chunks.isEmpty()) {
                LOG.error("Do sharded split failed, chunks of {} is empty.", namespace);
                throw new FlinkRuntimeException(
                        String.format(
                                "Do sharded split failed, chunks of %s is empty.", namespace));
            }
        } catch (MongoException e) {
            LOG.error(
                    "Read chunks from {} failed with error message: {}", namespace, e.getMessage());
            throw new FlinkRuntimeException(e);
        }

        List<MongoScanSourceSplit> sourceSplits = new ArrayList<>(chunks.size());
        for (int i = 0; i < chunks.size(); i++) {
            BsonDocument chunk = chunks.get(i);
            sourceSplits.add(
                    new MongoScanSourceSplit(
                            String.format("%s_%d", namespace, i),
                            namespace.getDatabaseName(),
                            namespace.getCollectionName(),
                            chunk.getDocument(MIN_FIELD),
                            chunk.getDocument(MAX_FIELD),
                            collectionMetadata.get().getDocument(KEY_FIELD)));
        }

        return sourceSplits;
    }
}
