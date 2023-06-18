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
import org.apache.flink.connector.mongodb.common.config.MongoConnectionOptions;
import org.apache.flink.connector.mongodb.source.split.MongoStreamOffset;

import com.mongodb.MongoNamespace;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.ascending;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.CLUSTER_TIME_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.DROPPED_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.KEY_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.MAX_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.MIN_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.NAMESPACE_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.SHARD_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.UUID_FIELD;

/** An util class with some helper method for MongoDB commands. */
@Internal
public class MongoUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MongoUtils.class);

    private static final String COLL_STATS_COMMAND = "collStats";
    private static final String SPLIT_VECTOR_COMMAND = "splitVector";
    private static final String KEY_PATTERN_OPTION = "keyPattern";
    private static final String MAX_CHUNK_SIZE_OPTION = "maxChunkSize";
    private static final String IS_MASTER_COMMAND = "isMaster";

    private static final String ADMIN_DATABASE = "admin";
    private static final String CONFIG_DATABASE = "config";
    private static final String COLLECTIONS_COLLECTION = "collections";
    private static final String CHUNKS_COLLECTION = "chunks";

    private MongoUtils() {}

    public static MongoClient clientFor(MongoConnectionOptions connectionOptions) {
        return MongoClients.create(connectionOptions.getUri());
    }

    public static <T> T doWithMongoClient(
            MongoConnectionOptions connectionOptions, Function<MongoClient, T> action) {
        try (MongoClient mongoClient = clientFor(connectionOptions)) {
            return action.apply(mongoClient);
        }
    }

    public static BsonDocument collStats(MongoClient mongoClient, MongoNamespace namespace) {
        BsonDocument collStatsCommand =
                new BsonDocument(COLL_STATS_COMMAND, new BsonString(namespace.getCollectionName()));
        return mongoClient
                .getDatabase(namespace.getDatabaseName())
                .runCommand(collStatsCommand, BsonDocument.class);
    }

    public static BsonDocument splitVector(
            MongoClient mongoClient,
            MongoNamespace namespace,
            BsonDocument keyPattern,
            int maxChunkSizeMB) {
        return splitVector(mongoClient, namespace, keyPattern, maxChunkSizeMB, null, null);
    }

    public static BsonDocument splitVector(
            MongoClient mongoClient,
            MongoNamespace namespace,
            BsonDocument keyPattern,
            int maxChunkSizeMB,
            @Nullable BsonDocument min,
            @Nullable BsonDocument max) {
        BsonDocument splitVectorCommand =
                new BsonDocument(SPLIT_VECTOR_COMMAND, new BsonString(namespace.getFullName()))
                        .append(KEY_PATTERN_OPTION, keyPattern)
                        .append(MAX_CHUNK_SIZE_OPTION, new BsonInt32(maxChunkSizeMB));
        Optional.ofNullable(min).ifPresent(v -> splitVectorCommand.append(MIN_FIELD, v));
        Optional.ofNullable(max).ifPresent(v -> splitVectorCommand.append(MAX_FIELD, v));
        return mongoClient
                .getDatabase(namespace.getDatabaseName())
                .runCommand(splitVectorCommand, BsonDocument.class);
    }

    public static Optional<BsonDocument> readCollectionMetadata(
            MongoClient mongoClient, MongoNamespace namespace) {
        MongoCollection<BsonDocument> collections =
                mongoClient
                        .getDatabase(CONFIG_DATABASE)
                        .getCollection(COLLECTIONS_COLLECTION)
                        .withDocumentClass(BsonDocument.class);

        return Optional.ofNullable(
                collections
                        .find(eq(ID_FIELD, namespace.getFullName()))
                        .projection(include(ID_FIELD, UUID_FIELD, DROPPED_FIELD, KEY_FIELD))
                        .first());
    }

    public static boolean isShardedCollectionDropped(BsonDocument collectionMetadata) {
        return collectionMetadata.getBoolean(DROPPED_FIELD, BsonBoolean.FALSE).getValue();
    }

    public static List<BsonDocument> readChunks(
            MongoClient mongoClient, BsonDocument collectionMetadata) {
        MongoCollection<BsonDocument> chunks =
                mongoClient
                        .getDatabase(CONFIG_DATABASE)
                        .getCollection(CHUNKS_COLLECTION)
                        .withDocumentClass(BsonDocument.class);

        Bson filter =
                or(
                        new BsonDocument(NAMESPACE_FIELD, collectionMetadata.get(ID_FIELD)),
                        // MongoDB 4.9.0 removed ns field of config.chunks collection, using
                        // collection's uuid instead.
                        // See: https://jira.mongodb.org/browse/SERVER-53105
                        new BsonDocument(UUID_FIELD, collectionMetadata.get(UUID_FIELD)));

        return chunks.find(filter)
                .projection(include(MIN_FIELD, MAX_FIELD, SHARD_FIELD))
                .sort(ascending(MIN_FIELD))
                .into(new ArrayList<>());
    }

    public static Bson project(List<String> projectedFields) {
        if (projectedFields.contains(ID_FIELD)) {
            return include(projectedFields);
        } else {
            // Creates a projection that excludes the _id field.
            // This suppresses the automatic inclusion of _id that is default.
            return fields(include(projectedFields), excludeId());
        }
    }

    public static MongoStreamOffset displayCurrentOffset(MongoConnectionOptions connectionOptions) {
        return doWithMongoClient(
                connectionOptions,
                client -> {
                    ChangeStreamIterable<Document> changeStreamIterable =
                            getChangeStreamIterable(
                                    client,
                                    connectionOptions.getDatabase(),
                                    connectionOptions.getCollection());

                    try (MongoChangeStreamCursor<ChangeStreamDocument<Document>>
                            changeStreamCursor = changeStreamIterable.cursor()) {
                        ChangeStreamDocument<?> firstResult = changeStreamCursor.tryNext();
                        BsonDocument resumeToken =
                                firstResult != null
                                        ? firstResult.getResumeToken()
                                        : changeStreamCursor.getResumeToken();

                        // Nullable when no change record or postResumeToken (new in MongoDB 4.0.7).
                        return Optional.ofNullable(resumeToken)
                                .map(MongoStreamOffset::fromResumeToken)
                                .orElse(
                                        MongoStreamOffset.fromClusterTime(
                                                currentClusterTime(client)));
                    }
                });
    }

    public static BsonTimestamp currentClusterTime(MongoClient mongoClient) {
        return isMaster(mongoClient)
                .getDocument("$" + CLUSTER_TIME_FIELD)
                .getTimestamp(CLUSTER_TIME_FIELD);
    }

    public static BsonDocument isMaster(MongoClient mongoClient) {
        BsonDocument isMasterCommand = new BsonDocument(IS_MASTER_COMMAND, new BsonInt32(1));
        return mongoClient
                .getDatabase(ADMIN_DATABASE)
                .runCommand(isMasterCommand, BsonDocument.class);
    }

    public static ChangeStreamIterable<Document> getChangeStreamIterable(
            MongoClient mongoClient, @Nullable String database, @Nullable String collection) {
        ChangeStreamIterable<Document> changeStream;
        if (StringUtils.isNotEmpty(database) && StringUtils.isNotEmpty(collection)) {
            MongoCollection<Document> coll =
                    mongoClient.getDatabase(database).getCollection(collection);
            LOG.info("Preparing change stream for collection {}.{}", database, collection);
            changeStream = coll.watch();
        } else if (StringUtils.isNotEmpty(database)) {
            MongoDatabase db = mongoClient.getDatabase(database);
            LOG.info("Preparing change stream for database {}", database);
            changeStream = db.watch();
        } else {
            LOG.info("Preparing change stream for deployment");
            changeStream = mongoClient.watch();
        }
        return changeStream;
    }
}
