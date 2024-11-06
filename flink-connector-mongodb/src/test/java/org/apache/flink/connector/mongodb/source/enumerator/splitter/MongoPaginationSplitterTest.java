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
import org.apache.flink.configuration.MemorySize;
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

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MAX_BOUNDARY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MAX_KEY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MIN_BOUNDARY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MIN_KEY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_HINT;
import static org.assertj.core.api.Assertions.assertThat;

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
        coll.insertMany(initializeRecords());
    }

    @AfterAll
    static void afterAll() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    ///  Test cases that specifies number of records in each partition explicitly.
    @Test
    void testSingleSplitPartitions() {
        MongoSplitContext splitContext = createSplitContext(TOTAL_RECORDS_COUNT);
        assertThat(new ArrayList<>(MongoPaginationSplitter.split(splitContext)))
                .isEqualTo(SINGLE_SPLIT);
    }

    @Test
    void testLargePartitionRecordSize() {
        MongoSplitContext splitContext = createSplitContext(TOTAL_RECORDS_COUNT * 2);
        assertThat(new ArrayList<>(MongoPaginationSplitter.split(splitContext)))
                .isEqualTo(SINGLE_SPLIT);
    }

    @Test
    void testLargerSizedPartitions() {
        MongoSplitContext splitContext = createSplitContext(15);
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
            MongoSplitContext splitContext = createSplitContext(TOTAL_RECORDS_COUNT - 1);
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
            MongoSplitContext splitContext = createSplitContext(TOTAL_RECORDS_COUNT);
            assertThat(new ArrayList<>(MongoPaginationSplitter.split(splitContext)))
                    .isEqualTo(SINGLE_SPLIT);
        }
    }

    ///  Test cases that do not specify number of records, and estimates record size with
    /// `avgObjSize`.
    @Test
    void testEstimatedSingleSplitPartitions() {
        MongoSplitContext splitContext =
                createSplitContext(MemorySize.ofMebiBytes(16), MemorySize.ZERO);
        assertThat(new ArrayList<>(MongoPaginationSplitter.split(splitContext)))
                .isEqualTo(SINGLE_SPLIT);
    }

    @Test
    void testEstimatedLargerSizedPartitions() {
        MongoSplitContext splitContext =
                createSplitContext(MemorySize.ofMebiBytes(50), MemorySize.ofMebiBytes(3));

        assertThat(new ArrayList<>(MongoPaginationSplitter.split(splitContext)))
                .isEqualTo(
                        createReferenceSplits(
                                Arrays.asList(
                                        Tuple2.of(BSON_MIN_KEY, new BsonInt64(16)),
                                        Tuple2.of(new BsonInt64(16), new BsonInt64(32)),
                                        Tuple2.of(new BsonInt64(32), new BsonInt64(48)),
                                        Tuple2.of(new BsonInt64(48), new BsonInt64(64)),
                                        Tuple2.of(new BsonInt64(64), new BsonInt64(80)),
                                        Tuple2.of(new BsonInt64(80), new BsonInt64(96)),
                                        Tuple2.of(new BsonInt64(96), new BsonInt64(112)),
                                        Tuple2.of(new BsonInt64(112), BSON_MAX_KEY))));
    }

    @Test
    void testEstimatedOffByOnePartitions() {
        {
            MongoSplitContext splitContext =
                    createSplitContext(
                            MemorySize.ofMebiBytes(TOTAL_RECORDS_COUNT - 1),
                            MemorySize.ofMebiBytes(1));
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
                    createSplitContext(
                            MemorySize.ofMebiBytes(TOTAL_RECORDS_COUNT), MemorySize.ofMebiBytes(1));
            assertThat(new ArrayList<>(MongoPaginationSplitter.split(splitContext)))
                    .isEqualTo(SINGLE_SPLIT);
        }
    }

    @Test
    void testEstimateWithoutAvgObjSize() {
        MongoSplitContext splitContext =
                createSplitContext(MemorySize.ofMebiBytes(1), MemorySize.ZERO);
        assertThat(new ArrayList<>(MongoPaginationSplitter.split(splitContext)))
                .isEqualTo(SINGLE_SPLIT);
    }

    private static List<BsonDocument> initializeRecords() {
        return IntStream.range(0, MongoPaginationSplitterTest.TOTAL_RECORDS_COUNT)
                .mapToObj(
                        idx ->
                                new BsonDocument("_id", new BsonInt64(idx))
                                        .append(
                                                "str",
                                                new BsonString(String.format("Record #%d", idx))))
                .collect(Collectors.toList());
    }

    private static MongoSplitContext createSplitContext(
            MemorySize partitionSize, MemorySize avgObjSize) {
        long avgObjSizeInBytes = avgObjSize.getBytes();
        return new MongoSplitContext(
                MongoReadOptions.builder().setPartitionSize(partitionSize).build(),
                mongoClient,
                TEST_NS,
                false,
                TOTAL_RECORDS_COUNT,
                (long) TOTAL_RECORDS_COUNT * avgObjSizeInBytes,
                avgObjSizeInBytes);
    }

    private static MongoSplitContext createSplitContext(int partitionRecordSize) {
        return new MongoSplitContext(
                MongoReadOptions.builder().setPartitionRecordSize(partitionRecordSize).build(),
                mongoClient,
                TEST_NS,
                false,
                MongoPaginationSplitterTest.TOTAL_RECORDS_COUNT,
                0,
                0);
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

    private static final List<MongoScanSourceSplit> SINGLE_SPLIT =
            Collections.singletonList(
                    new MongoScanSourceSplit(
                            TEST_NS.getFullName(),
                            TEST_NS.getDatabaseName(),
                            TEST_NS.getCollectionName(),
                            BSON_MIN_BOUNDARY,
                            BSON_MAX_BOUNDARY,
                            ID_HINT));
}
