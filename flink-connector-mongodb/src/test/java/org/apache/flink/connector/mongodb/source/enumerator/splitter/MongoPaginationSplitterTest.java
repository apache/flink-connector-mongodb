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

package org.apache.flink.connector.mongodb.source.enumerator.splitter;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.mongodb.source.config.MongoReadOptions;
import org.apache.flink.connector.mongodb.source.split.MongoScanSourceSplit;
import org.apache.flink.connector.mongodb.testutils.MongoShardedContainers;
import org.apache.flink.connector.mongodb.testutils.MongoTestUtil;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.Network;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MAX_KEY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MIN_KEY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_HINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link MongoPaginationSplitter}. */
class MongoPaginationSplitterTest {

    @RegisterExtension
    private static final MongoShardedContainers MONGO_SHARDED_CONTAINER =
            MongoTestUtil.createMongoDBShardedContainers(Network.newNetwork());

    private static MongoClient mongoClient;

    private static final MongoNamespace TEST_NS = new MongoNamespace("test.test");

    private static final int TOTAL_RECORDS_COUNT = 120;

    @BeforeAll
    static void beforeAll() {
        mongoClient = MongoClients.create(MONGO_SHARDED_CONTAINER.getConnectionString());
        MongoCollection<BsonDocument> coll =
                mongoClient
                        .getDatabase(TEST_NS.getDatabaseName())
                        .getCollection(TEST_NS.getCollectionName())
                        .withDocumentClass(BsonDocument.class);
        coll.insertMany(createRecords(TOTAL_RECORDS_COUNT));
    }

    @AfterAll
    static void afterAll() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Test
    void testMissingArgument() {
        MongoSplitContext splitContext =
                new MongoSplitContext(
                        MongoReadOptions.builder().build(), mongoClient, TEST_NS, false, 0, 0, 0);
        assertThatThrownBy(() -> new ArrayList<>(MongoPaginationSplitter.split(splitContext)))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("The partition record size must be set to a positive integer.");
    }

    @Test
    void testSplitEmptyCollection() {
        MongoSplitContext splitContext =
                new MongoSplitContext(
                        MongoReadOptions.builder().setPartitionRecordSize(1).build(),
                        mongoClient,
                        TEST_NS,
                        false,
                        0,
                        0,
                        0);
        assertThat(new ArrayList<>(MongoPaginationSplitter.split(splitContext)))
                .isEqualTo(Collections.emptyList());
    }

    @Test
    void testSingleSplitPartitions() {
        MongoSplitContext splitContext =
                new MongoSplitContext(
                        MongoReadOptions.builder()
                                .setPartitionRecordSize(TOTAL_RECORDS_COUNT)
                                .build(),
                        mongoClient,
                        TEST_NS,
                        false,
                        TOTAL_RECORDS_COUNT,
                        0,
                        0);
        assertThat(new ArrayList<>(MongoPaginationSplitter.split(splitContext)))
                .isEqualTo(
                        createReferenceSplits(
                                Collections.singletonList(Tuple2.of(BSON_MIN_KEY, BSON_MAX_KEY))));
    }

    @Test
    void testLargerSizedPartitions() {
        MongoSplitContext splitContext =
                new MongoSplitContext(
                        MongoReadOptions.builder().setPartitionRecordSize(15).build(),
                        mongoClient,
                        TEST_NS,
                        false,
                        TOTAL_RECORDS_COUNT,
                        0,
                        0);
        assertThat(new ArrayList<>(MongoPaginationSplitter.split(splitContext)))
                .isEqualTo(
                        createReferenceSplits(
                                Arrays.asList(
                                        Tuple2.of(BSON_MIN_KEY, new BsonInt64(15)),
                                        Tuple2.of(new BsonInt64(15), new BsonInt64(30)),
                                        Tuple2.of(new BsonInt64(30), new BsonInt64(45)),
                                        Tuple2.of(new BsonInt64(45), new BsonInt64(60)),
                                        Tuple2.of(new BsonInt64(60), new BsonInt64(75)),
                                        Tuple2.of(new BsonInt64(75), new BsonInt64(90)),
                                        Tuple2.of(new BsonInt64(90), new BsonInt64(105)),
                                        Tuple2.of(new BsonInt64(105), BSON_MAX_KEY))));
    }

    @Test
    void testOffByOnePartitions() {
        {
            MongoSplitContext splitContext =
                    new MongoSplitContext(
                            MongoReadOptions.builder()
                                    .setPartitionRecordSize(TOTAL_RECORDS_COUNT - 1)
                                    .build(),
                            mongoClient,
                            TEST_NS,
                            false,
                            TOTAL_RECORDS_COUNT,
                            0,
                            0);
            assertThat(new ArrayList<>(MongoPaginationSplitter.split(splitContext)))
                    .isEqualTo(
                            createReferenceSplits(
                                    Arrays.asList(
                                            Tuple2.of(
                                                    BSON_MIN_KEY,
                                                    new BsonInt64(TOTAL_RECORDS_COUNT - 1)),
                                            Tuple2.of(
                                                    new BsonInt64(TOTAL_RECORDS_COUNT - 1),
                                                    BSON_MAX_KEY))));
        }

        {
            MongoSplitContext splitContext =
                    new MongoSplitContext(
                            MongoReadOptions.builder()
                                    .setPartitionRecordSize(TOTAL_RECORDS_COUNT)
                                    .build(),
                            mongoClient,
                            TEST_NS,
                            false,
                            TOTAL_RECORDS_COUNT,
                            0,
                            0);
            assertThat(new ArrayList<>(MongoPaginationSplitter.split(splitContext)))
                    .isEqualTo(
                            createReferenceSplits(
                                    Collections.singletonList(
                                            Tuple2.of(BSON_MIN_KEY, BSON_MAX_KEY))));
        }
    }

    private static List<BsonDocument> createRecords(int samplesCount) {
        return IntStream.range(0, samplesCount)
                .mapToObj(
                        idx ->
                                new BsonDocument("_id", new BsonInt64(idx))
                                        .append(
                                                "str",
                                                new BsonString(String.format("Record #%d", idx))))
                .collect(Collectors.toList());
    }

    private static List<MongoScanSourceSplit> createReferenceSplits(
            List<Tuple2<BsonValue, BsonValue>> ranges) {

        List<MongoScanSourceSplit> results = new ArrayList<>();
        for (int i = 0; i < ranges.size(); i++) {
            results.add(
                    new MongoScanSourceSplit(
                            "test.test_" + i,
                            "test",
                            "test",
                            new BsonDocument(ID_FIELD, ranges.get(i).f0),
                            new BsonDocument(ID_FIELD, ranges.get(i).f1),
                            ID_HINT));
        }
        return results;
    }
}
