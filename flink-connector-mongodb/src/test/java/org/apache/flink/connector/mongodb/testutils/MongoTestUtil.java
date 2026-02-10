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

package org.apache.flink.connector.mongodb.testutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.mongodb.table.MongoConnectorOptions;
import org.apache.flink.table.factories.FactoryUtil;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Collection of utility methods for MongoDB tests. */
@Internal
public class MongoTestUtil {

    public static final String MONGODB_HOSTNAME = "mongodb";

    public static final String MONGO_IMAGE_PREFIX = "mongo:";

    public static final String ADMIN_DATABASE = "admin";
    public static final String CONFIG_DATABASE = "config";
    public static final String SETTINGS_COLLECTION = "settings";
    public static final String CHUNK_SIZE_FIELD = "chunksize";
    public static final String VALUE_FIELD = "value";

    private MongoTestUtil() {}

    /**
     * Creates a preconfigured {@link MongoDBContainer}.
     *
     * @return configured MongoDB container
     */
    public static MongoDBContainer createMongoDBContainer() {
        return new MongoDBContainer(mongoDockerImageName());
    }

    /**
     * Creates a preconfigured {@link MongoDBContainer}.
     *
     * @param network for test containers
     * @return configured MongoDB sharded containers
     */
    public static MongoShardedContainers createMongoDBShardedContainers(Network network) {
        return new MongoShardedContainers(mongoDockerImageName(), network);
    }

    public static DockerImageName mongoDockerImageName() {
        return DockerImageName.parse(MONGO_IMAGE_PREFIX + mongoVersion());
    }

    public static String mongoVersion() {
        return System.getProperty("mongodb.version");
    }

    public static void assertThatIdsAreNotWritten(MongoCollection<Document> coll, Integer... ids) {
        List<Integer> idsAreWritten = new ArrayList<>();
        coll.find(Filters.in("_id", ids)).map(d -> d.getInteger("_id")).into(idsAreWritten);

        assertThat(idsAreWritten).isEmpty();
    }

    public static void assertThatIdsAreNotWritten(MongoCollection<Document> coll, String... ids) {
        List<String> idsAreWritten = new ArrayList<>();
        coll.find(Filters.in("_id", ids)).map(d -> d.getString("_id")).into(idsAreWritten);

        assertThat(idsAreWritten).isEmpty();
    }

    public static void assertThatIdsAreWrittenInAnyOrder(
            MongoCollection<Document> coll, Integer... ids) {
        List<Integer> actualIds = new ArrayList<>();
        coll.find(Filters.in("_id", ids)).map(d -> d.getInteger("_id")).into(actualIds);

        assertThat(actualIds).containsExactlyInAnyOrder(ids);
    }

    public static void assertThatIdsAreWrittenInAnyOrder(
            MongoCollection<Document> coll, String... ids) {
        List<String> actualIds = new ArrayList<>();
        coll.find(Filters.in("_id", ids)).map(d -> d.getString("_id")).into(actualIds);

        assertThat(actualIds).containsExactlyInAnyOrder(ids);
    }

    public static void assertThatIdsAreWrittenInOrder(
            MongoCollection<Document> coll, Integer... ids) {
        List<Integer> actualIds = new ArrayList<>();
        coll.find(Filters.in("_id", ids)).map(d -> d.getInteger("_id")).into(actualIds);

        assertThat(actualIds).containsExactly(ids);
    }

    public static void assertThatIdsAreWrittenWithMaxWaitTime(
            MongoCollection<Document> coll, long maxWaitTimeMs, Integer... ids)
            throws InterruptedException {
        long startTimeMillis = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTimeMillis < maxWaitTimeMs) {
            if (coll.countDocuments(Filters.in("_id", ids)) == ids.length) {
                break;
            }
            Thread.sleep(1000L);
        }
        assertThatIdsAreWrittenInAnyOrder(coll, ids);
    }

    public static String getConnectorSql(
            String database, String collection, String connectionString) {
        return String.format("'%s'='%s',\n", FactoryUtil.CONNECTOR.key(), "mongodb")
                + String.format("'%s'='%s',\n", MongoConnectorOptions.URI.key(), connectionString)
                + String.format("'%s'='%s',\n", MongoConnectorOptions.DATABASE.key(), database)
                + String.format("'%s'='%s'\n", MongoConnectorOptions.COLLECTION.key(), collection);
    }

    public static void createIndex(
            MongoClient mongoClient,
            String databaseName,
            String collectionName,
            Bson keys,
            IndexOptions indexOptions) {
        mongoClient
                .getDatabase(databaseName)
                .getCollection(collectionName)
                .createIndex(keys, indexOptions);
    }

    public static void shardCollection(
            MongoClient mongoClient, String databaseName, String collectionName, Bson keys) {
        MongoDatabase admin = mongoClient.getDatabase(ADMIN_DATABASE);
        Document enableShardingCommand = new Document("enableSharding", databaseName);
        admin.runCommand(enableShardingCommand);

        Document shardCollectionCommand =
                new Document("shardCollection", databaseName + "." + collectionName)
                        .append("key", keys);
        admin.runCommand(shardCollectionCommand);
    }
}
