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

package org.apache.flink.connector.mongodb;

import org.apache.flink.annotation.Internal;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.slf4j.Logger;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Collection of utility methods for MongoDB tests. */
@Internal
public class MongoTestUtil {

    private static final String MONGO_4_0 = "mongo:4.0.10";

    private MongoTestUtil() {}

    /**
     * Creates a preconfigured {@link MongoDBContainer}.
     *
     * @param logger for test containers
     * @return configured MongoDB container
     */
    public static MongoDBContainer createMongoDBContainer(Logger logger) {
        return new MongoDBContainer(DockerImageName.parse(MONGO_4_0))
                .withLogConsumer(new Slf4jLogConsumer(logger));
    }

    public static void assertThatIdsAreNotWritten(MongoCollection<Document> coll, Integer... ids) {
        boolean existOne = coll.find(Filters.in("_id", ids)).first() != null;
        assertThat(existOne).isFalse();
    }

    public static void assertThatIdsAreWritten(MongoCollection<Document> coll, Integer... ids) {
        List<Integer> actualIds = new ArrayList<>();
        coll.find(Filters.in("_id", ids)).map(d -> d.getInteger("_id")).into(actualIds);

        assertThat(actualIds).containsExactlyInAnyOrder(ids);
    }
}
