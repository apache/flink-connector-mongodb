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

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.slf4j.Logger;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Collection of utility methods for MongoDB tests. */
@Internal
public class MongoTestUtil {

    public static final String MONGODB_HOSTNAME = "mongodb";

    public static final String MONGO_IMAGE_PREFIX = "mongo:";
    public static final String MONGO_4_0 = "4.0.10";
    public static final String MONGO_5_0 = "5.0.2";
    public static final String MONGO_6_0 = "6.0.6";

    private MongoTestUtil() {}

    /**
     * Creates a preconfigured {@link MongoDBContainer}.
     *
     * @param logger for test containers
     * @return configured MongoDB container
     */
    public static MongoDBContainer createMongoDBContainer(Logger logger) {
        return createMongoDBContainer(MONGO_IMAGE_PREFIX + MONGO_4_0, logger);
    }

    /**
     * Creates a preconfigured {@link MongoDBContainer}.
     *
     * @param imageName mongo docker image name
     * @param logger for test containers
     * @return configured MongoDB container
     */
    public static MongoDBContainer createMongoDBContainer(String imageName, Logger logger) {
        return new MongoDBContainer(DockerImageName.parse(imageName))
                .withLogConsumer(new Slf4jLogConsumer(logger));
    }

    /**
     * Creates a preconfigured {@link MongoDBContainer}.
     *
     * @param network for test containers
     * @return configured MongoDB sharded containers
     */
    public static MongoShardedContainers createMongoDBShardedContainers(Network network) {
        return createMongoDBShardedContainers(MONGO_IMAGE_PREFIX + MONGO_4_0, network);
    }

    /**
     * Creates a preconfigured {@link MongoDBContainer}.
     *
     * @param imageName mongo docker image name
     * @param network for test containers
     * @return configured MongoDB sharded containers
     */
    public static MongoShardedContainers createMongoDBShardedContainers(
            String imageName, Network network) {
        return new MongoShardedContainers(DockerImageName.parse(imageName), network);
    }

    public static void assertThatIdsAreNotWritten(MongoCollection<Document> coll, Integer... ids) {
        List<Integer> idsAreWritten = new ArrayList<>();
        coll.find(Filters.in("_id", ids)).map(d -> d.getInteger("_id")).into(idsAreWritten);

        assertThat(idsAreWritten).isEmpty();
    }

    public static void assertThatIdsAreWritten(MongoCollection<Document> coll, Integer... ids) {
        List<Integer> actualIds = new ArrayList<>();
        coll.find(Filters.in("_id", ids)).map(d -> d.getInteger("_id")).into(actualIds);

        assertThat(actualIds).containsExactlyInAnyOrder(ids);
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
        assertThatIdsAreWritten(coll, ids);
    }
}
