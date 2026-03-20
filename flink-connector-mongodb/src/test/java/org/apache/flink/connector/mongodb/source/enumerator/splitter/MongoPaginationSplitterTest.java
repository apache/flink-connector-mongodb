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
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationStrength;
import com.mongodb.client.model.IndexOptions;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.Network;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.AND_OPERATOR;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MAX_BOUNDARY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MAX_KEY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MIN_BOUNDARY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MIN_KEY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.EQ_OPERATOR;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_HINT;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link MongoPaginationSplitter}. */
class MongoPaginationSplitterTest {

    @RegisterExtension
    private static final MongoShardedContainers MONGO_SHARDED_CONTAINER =
            MongoTestUtil.createMongoDBShardedContainers(Network.newNetwork());

    private static MongoClient mongoClient;

    private static MongoCollection<BsonDocument> filteredCollection;

    private static final MongoNamespace TEST_NS = new MongoNamespace("test.test");

    private static final MongoNamespace FILTER_TEST_NS =
            new MongoNamespace("test", "pagination_filter");

    private static final String GRP_FIELD = "grp";

    private static final BsonDocument GRP_ID_INDEX_HINT =
            new BsonDocument(GRP_FIELD, new BsonInt32(1)).append(ID_FIELD, new BsonInt32(1));

    private static final BsonDocument GRP_ID_EXTRA_INDEX_HINT =
            new BsonDocument(GRP_FIELD, new BsonInt32(1))
                    .append(ID_FIELD, new BsonInt32(1))
                    .append("extra", new BsonInt32(1));

    private static final int TOTAL_RECORDS_COUNT = 120;

    // Documents with {@link #GRP_FIELD} {@code 0} (ids 0–59) or {@code 1} (ids 60–119).
    private static final int FILTERED_GROUP0_COUNT = 60;

    @BeforeAll
    static void beforeAll() {
        mongoClient = MongoClients.create(MONGO_SHARDED_CONTAINER.getConnectionString());
        MongoCollection<BsonDocument> coll =
                mongoClient
                        .getDatabase(TEST_NS.getDatabaseName())
                        .getCollection(TEST_NS.getCollectionName())
                        .withDocumentClass(BsonDocument.class);
        coll.insertMany(initializeRecords());

        filteredCollection =
                mongoClient
                        .getDatabase(FILTER_TEST_NS.getDatabaseName())
                        .getCollection(FILTER_TEST_NS.getCollectionName())
                        .withDocumentClass(BsonDocument.class);

        filteredCollection.insertMany(initializeFilteredRecords());

        MongoTestUtil.createIndex(
                mongoClient,
                FILTER_TEST_NS.getDatabaseName(),
                FILTER_TEST_NS.getCollectionName(),
                new Document(GRP_FIELD, 1).append(ID_FIELD, 1),
                new IndexOptions());
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

    // --- Splitting with a filter (compound index {grp, _id}) ---

    @Test
    void testFilteredPartition_largerSizedPartitions() {
        BsonDocument filter = new BsonDocument(GRP_FIELD, new BsonInt32(0));
        MongoSplitContext splitContext = createFilteredSplitContext(filter, 15);
        assertThat(new ArrayList<>(MongoPaginationSplitter.split(splitContext)))
                .isEqualTo(
                        createFilteredReferenceSplits(
                                Arrays.asList(
                                        Tuple2.of(BSON_MIN_KEY, new BsonInt64(15)),
                                        Tuple2.of(new BsonInt64(15), new BsonInt64(30)),
                                        Tuple2.of(new BsonInt64(30), new BsonInt64(45)),
                                        Tuple2.of(new BsonInt64(45), BSON_MAX_KEY))));
    }

    @Test
    void testFilteredPartition_singleSplitWhenPartitionCoversFilteredCount() {
        BsonDocument filter = new BsonDocument(GRP_FIELD, new BsonInt32(0));
        MongoSplitContext splitContext = createFilteredSplitContext(filter, FILTERED_GROUP0_COUNT);
        assertThat(new ArrayList<>(MongoPaginationSplitter.split(splitContext)))
                .isEqualTo(FILTERED_SINGLE_SPLIT);
    }

    @Test
    void testFilteredPartition_offByOnePartitions() {
        BsonDocument filter = new BsonDocument(GRP_FIELD, new BsonInt32(0));
        {
            MongoSplitContext splitContext =
                    createFilteredSplitContext(filter, FILTERED_GROUP0_COUNT - 1);
            assertThat(new ArrayList<>(MongoPaginationSplitter.split(splitContext)))
                    .isEqualTo(
                            createFilteredReferenceSplits(
                                    Arrays.asList(
                                            Tuple2.of(BSON_MIN_KEY, new BsonInt64(59)),
                                            Tuple2.of(new BsonInt64(59), BSON_MAX_KEY))));
        }
        {
            MongoSplitContext splitContext =
                    createFilteredSplitContext(filter, FILTERED_GROUP0_COUNT);
            assertThat(new ArrayList<>(MongoPaginationSplitter.split(splitContext)))
                    .isEqualTo(FILTERED_SINGLE_SPLIT);
        }
    }

    @Test
    void testFilteredPartition_equalityFilterWrappedInAnd() {
        BsonDocument filter =
                new BsonDocument(
                        AND_OPERATOR,
                        new BsonArray(
                                Collections.singletonList(
                                        new BsonDocument(GRP_FIELD, new BsonInt32(0)))));
        MongoSplitContext splitContext = createFilteredSplitContext(filter, 15);
        assertThat(new ArrayList<>(MongoPaginationSplitter.split(splitContext)))
                .isEqualTo(
                        createFilteredReferenceSplits(
                                Arrays.asList(
                                        Tuple2.of(BSON_MIN_KEY, new BsonInt64(15)),
                                        Tuple2.of(new BsonInt64(15), new BsonInt64(30)),
                                        Tuple2.of(new BsonInt64(30), new BsonInt64(45)),
                                        Tuple2.of(new BsonInt64(45), BSON_MAX_KEY))));
    }

    @Test
    void testFilteredPartition_eqOperatorUsesLiteralValueInBounds() {
        BsonDocument filter =
                new BsonDocument(GRP_FIELD, new BsonDocument(EQ_OPERATOR, new BsonInt32(0)));
        MongoSplitContext splitContext = createFilteredSplitContext(filter, 15);
        assertThat(new ArrayList<>(MongoPaginationSplitter.split(splitContext)))
                .isEqualTo(
                        createFilteredReferenceSplits(
                                Arrays.asList(
                                        Tuple2.of(BSON_MIN_KEY, new BsonInt64(15)),
                                        Tuple2.of(new BsonInt64(15), new BsonInt64(30)),
                                        Tuple2.of(new BsonInt64(30), new BsonInt64(45)),
                                        Tuple2.of(new BsonInt64(45), BSON_MAX_KEY))));
    }

    @Test
    void testEstimatedRecordSizeLargerThanPartitionSize() {
        // Partition size / average object size rounds down to 0, still at least 1 record per split
        MongoSplitContext splitContext =
                createSplitContext(MemorySize.ofMebiBytes(1), MemorySize.ofMebiBytes(2));
        assertThat(MongoPaginationSplitter.split(splitContext)).hasSize(TOTAL_RECORDS_COUNT);
    }

    // --- @VisibleForTesting: extractEqualityFields ---

    @Test
    void filterMatchingIndexKeys_includesEqualityFromAnd() {
        BsonDocument indexHint =
                new BsonDocument("a", new BsonInt32(1)).append(ID_FIELD, new BsonInt32(1));
        BsonDocument filter =
                new BsonDocument(
                        AND_OPERATOR,
                        new BsonArray(
                                Collections.singletonList(
                                        new BsonDocument("a", new BsonInt32(1)))));
        BsonDocument actual = MongoPaginationSplitter.filterMatchingIndexKeys(indexHint, filter);
        BsonDocument expected = new BsonDocument("a", new BsonInt32(1));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void filterMatchingIndexKeys_unwrapsEqAndExcludesKeysAfterId() {
        BsonDocument filter =
                new BsonDocument(GRP_FIELD, new BsonDocument(EQ_OPERATOR, new BsonInt32(0)))
                        .append("extra", new BsonInt32(1));
        BsonDocument actual =
                MongoPaginationSplitter.filterMatchingIndexKeys(GRP_ID_EXTRA_INDEX_HINT, filter);
        assertThat(actual).isEqualTo(new BsonDocument(GRP_FIELD, new BsonInt32(0)));
    }

    // --- createIndexBound: trailing index keys after _id ---

    @Test
    void createIndexBound_trailingFilterKeyAfterIdUsesWildcard() {
        BsonDocument filter =
                new BsonDocument(GRP_FIELD, new BsonInt32(0)).append("extra", new BsonInt32(1));
        BsonDocument actual =
                MongoPaginationSplitter.createIndexBound(
                        GRP_ID_EXTRA_INDEX_HINT, filter, new BsonInt64(15), BSON_MAX_KEY);
        BsonDocument expected =
                new BsonDocument(GRP_FIELD, new BsonInt32(0))
                        .append(ID_FIELD, new BsonInt64(15))
                        .append("extra", BSON_MAX_KEY);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void createIndexBound_trailingKeysAfterIdUseOtherValuesPerBoundSide() {
        BsonDocument filter = new BsonDocument(GRP_FIELD, new BsonInt32(0));

        BsonDocument actualMinSide =
                MongoPaginationSplitter.createIndexBound(
                        GRP_ID_EXTRA_INDEX_HINT, filter, new BsonInt64(15), BSON_MIN_KEY);
        BsonDocument expectedMinSide =
                new BsonDocument(GRP_FIELD, new BsonInt32(0))
                        .append(ID_FIELD, new BsonInt64(15))
                        .append("extra", BSON_MIN_KEY);
        assertThat(actualMinSide).isEqualTo(expectedMinSide);

        BsonDocument actualMaxSide =
                MongoPaginationSplitter.createIndexBound(
                        GRP_ID_EXTRA_INDEX_HINT, filter, new BsonInt64(15), BSON_MAX_KEY);
        BsonDocument expectedMaxSide =
                new BsonDocument(GRP_FIELD, new BsonInt32(0))
                        .append(ID_FIELD, new BsonInt64(15))
                        .append("extra", BSON_MAX_KEY);
        assertThat(actualMaxSide).isEqualTo(expectedMaxSide);
    }

    @Test
    void createIndexBound_trailingKeysUseMinOrMaxWildcardForFullRangeEnds() {
        BsonDocument filter = new BsonDocument(GRP_FIELD, new BsonInt32(0));

        BsonDocument actualFullMin =
                MongoPaginationSplitter.createIndexBound(
                        GRP_ID_EXTRA_INDEX_HINT, filter, BSON_MIN_KEY, BSON_MIN_KEY);
        BsonDocument expectedFullMin =
                new BsonDocument(GRP_FIELD, new BsonInt32(0))
                        .append(ID_FIELD, BSON_MIN_KEY)
                        .append("extra", BSON_MIN_KEY);
        assertThat(actualFullMin).isEqualTo(expectedFullMin);

        BsonDocument actualFullMax =
                MongoPaginationSplitter.createIndexBound(
                        GRP_ID_EXTRA_INDEX_HINT, filter, BSON_MAX_KEY, BSON_MAX_KEY);
        BsonDocument expectedFullMax =
                new BsonDocument(GRP_FIELD, new BsonInt32(0))
                        .append(ID_FIELD, BSON_MAX_KEY)
                        .append("extra", BSON_MAX_KEY);
        assertThat(actualFullMax).isEqualTo(expectedFullMax);
    }

    @Test
    void createIndexBound_nullFilterContainsOnlyId() {
        BsonDocument actual =
                MongoPaginationSplitter.createIndexBound(
                        ID_HINT, null, new BsonInt64(7), BSON_MIN_KEY);
        BsonDocument expected = new BsonDocument(ID_FIELD, new BsonInt64(7));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void extractEqualityFields_includesTopLevelScalarPredicates() {
        BsonDocument filter =
                new BsonDocument("a", new BsonInt32(1)).append("b", new BsonString("x"));
        assertThat(MongoPaginationSplitter.extractEqualityFields(filter))
                .containsExactlyInAnyOrder("a", "b");
    }

    @Test
    void extractEqualityFields_includesEqOnly() {
        BsonDocument filter =
                new BsonDocument("a", new BsonDocument(EQ_OPERATOR, new BsonInt32(1)));
        assertThat(MongoPaginationSplitter.extractEqualityFields(filter)).containsExactly("a");
    }

    @Test
    void extractEqualityFields_excludesRangeAndComplexDocuments() {
        BsonDocument filter =
                new BsonDocument("a", new BsonDocument("$gt", new BsonInt32(0)))
                        .append("b", new BsonDocument("$in", new BsonArray()));
        assertThat(MongoPaginationSplitter.extractEqualityFields(filter)).isEmpty();
    }

    @Test
    void extractEqualityFields_excludesRegexArrayDocumentAndOperatorKeys() {
        BsonArray array = new BsonArray(Arrays.asList(new BsonInt32(1), new BsonInt32(2)));
        BsonDocument filter =
                new BsonDocument("regex", new BsonRegularExpression("^a"))
                        .append("array", array)
                        .append("eqArray", new BsonDocument(EQ_OPERATOR, array))
                        .append("embedded", new BsonDocument("x", new BsonInt32(1)))
                        .append("$comment", new BsonString("note"))
                        .append(
                                "$or",
                                new BsonArray(
                                        Collections.singletonList(
                                                new BsonDocument("o", new BsonInt32(1)))));
        assertThat(MongoPaginationSplitter.extractEqualityFields(filter)).isEmpty();
    }

    @Test
    void extractEqualityFields_flattensAndArray() {
        BsonDocument filter =
                new BsonDocument(
                        AND_OPERATOR,
                        new BsonArray(
                                Arrays.asList(
                                        new BsonDocument("x", new BsonInt32(1)),
                                        new BsonDocument("y", new BsonInt32(2)))));
        assertThat(MongoPaginationSplitter.extractEqualityFields(filter))
                .containsExactlyInAnyOrder("x", "y");
    }

    @Test
    void extractEqualityFields_combinesTopLevelAndNestedAnd() {
        BsonDocument filter =
                new BsonDocument("top", new BsonInt32(0))
                        .append(
                                AND_OPERATOR,
                                new BsonArray(
                                        Collections.singletonList(
                                                new BsonDocument("inner", new BsonInt32(1)))));
        assertThat(MongoPaginationSplitter.extractEqualityFields(filter))
                .containsExactlyInAnyOrder("top", "inner");
    }

    // --- @VisibleForTesting: findBestSupportingIndex ---

    @Test
    void findBestSupportingIndex_nullFilterReturnsIdHint() {
        assertThat(MongoPaginationSplitter.findBestSupportingIndex(filteredCollection, null))
                .isEqualTo(ID_HINT);
    }

    @Test
    void findBestSupportingIndex_prefersCompoundIndexWithLongerMatchingPrefix() {
        // Collection for test
        String collName = "find_best_index_compound";
        mongoClient.getDatabase(TEST_NS.getDatabaseName()).getCollection(collName).drop();

        MongoTestUtil.createIndex(
                mongoClient,
                TEST_NS.getDatabaseName(),
                collName,
                new Document("a", 1).append("_id", 1),
                new IndexOptions());

        MongoCollection<BsonDocument> coll =
                mongoClient
                        .getDatabase(TEST_NS.getDatabaseName())
                        .getCollection(collName)
                        .withDocumentClass(BsonDocument.class);

        coll.insertOne(new BsonDocument("_id", new BsonInt64(1)).append("a", new BsonInt32(1)));

        BsonDocument filter = new BsonDocument("a", new BsonInt32(42));
        BsonDocument hint = MongoPaginationSplitter.findBestSupportingIndex(coll, filter);

        assertThat(hint.keySet()).containsExactlyInAnyOrder("a", "_id");
        assertThat(hint.getInt32("a").getValue()).isEqualTo(1);
        assertThat(hint.getInt32("_id").getValue()).isEqualTo(1);
    }

    @Test
    void findBestSupportingIndex_fallsBackToIdIndexWhenCompoundDoesNotMatchFilter() {
        // Collection for test
        String collName = "find_best_index_fallback";
        mongoClient.getDatabase(TEST_NS.getDatabaseName()).getCollection(collName).drop();

        MongoTestUtil.createIndex(
                mongoClient,
                TEST_NS.getDatabaseName(),
                collName,
                new Document("b", 1).append("_id", 1),
                new IndexOptions());

        MongoCollection<BsonDocument> coll =
                mongoClient
                        .getDatabase(TEST_NS.getDatabaseName())
                        .getCollection(collName)
                        .withDocumentClass(BsonDocument.class);

        coll.insertOne(new BsonDocument("_id", new BsonInt64(1)).append("b", new BsonInt32(1)));

        BsonDocument filter = new BsonDocument("a", new BsonInt32(1));
        BsonDocument hint = MongoPaginationSplitter.findBestSupportingIndex(coll, filter);

        assertThat(hint).isEqualTo(ID_HINT);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unsupportedIndexes")
    void findBestSupportingIndex_skipsUnsupportedIndexes(
            String name, Document keys, IndexOptions options) {

        String collName = "find_best_index_unsupported_" + name;
        mongoClient.getDatabase(TEST_NS.getDatabaseName()).getCollection(collName).drop();

        MongoTestUtil.createIndex(mongoClient, TEST_NS.getDatabaseName(), collName, keys, options);

        MongoCollection<BsonDocument> coll =
                mongoClient
                        .getDatabase(TEST_NS.getDatabaseName())
                        .getCollection(collName)
                        .withDocumentClass(BsonDocument.class);

        BsonDocument filter = new BsonDocument("a", new BsonInt32(1));
        assertThat(MongoPaginationSplitter.findBestSupportingIndex(coll, filter))
                .isEqualTo(ID_HINT);
    }

    private static Stream<Arguments> unsupportedIndexes() {
        return Stream.of(
                Arguments.of(
                        "descendingId",
                        new Document("a", 1).append(ID_FIELD, -1),
                        new IndexOptions()),
                Arguments.of(
                        "hashed",
                        new Document("a", "hashed").append(ID_FIELD, 1),
                        new IndexOptions()),
                Arguments.of(
                        "partial",
                        new Document("a", 1).append(ID_FIELD, 1),
                        new IndexOptions().partialFilterExpression(new Document("keep", true))),
                Arguments.of(
                        "collation",
                        new Document("a", 1).append(ID_FIELD, 1),
                        new IndexOptions()
                                .collation(
                                        Collation.builder()
                                                .locale("en")
                                                .collationStrength(CollationStrength.SECONDARY)
                                                .build())));
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

    private static List<BsonDocument> initializeFilteredRecords() {
        return IntStream.range(0, TOTAL_RECORDS_COUNT)
                .mapToObj(
                        idx ->
                                new BsonDocument("_id", new BsonInt64(idx))
                                        .append(
                                                GRP_FIELD,
                                                new BsonInt32(idx < FILTERED_GROUP0_COUNT ? 0 : 1))
                                        .append(
                                                "str",
                                                new BsonString(String.format("Record #%d", idx))))
                .collect(Collectors.toList());
    }

    private static MongoSplitContext createFilteredSplitContext(
            BsonDocument filter, int partitionRecordSize) {
        return new MongoSplitContext(
                MongoReadOptions.builder()
                        .setPartitionRecordSize(partitionRecordSize)
                        .setFilter(filter)
                        .build(),
                mongoClient,
                FILTER_TEST_NS,
                false,
                0,
                0,
                0);
    }

    private static List<MongoScanSourceSplit> createFilteredReferenceSplits(
            List<Tuple2<BsonValue, BsonValue>> ranges) {
        BsonInt32 grp0 = new BsonInt32(0);
        List<MongoScanSourceSplit> results = new ArrayList<>();
        for (int i = 0; i < ranges.size(); i++) {
            results.add(
                    new MongoScanSourceSplit(
                            FILTER_TEST_NS.getFullName() + "_" + i,
                            FILTER_TEST_NS.getDatabaseName(),
                            FILTER_TEST_NS.getCollectionName(),
                            new BsonDocument(GRP_FIELD, grp0).append(ID_FIELD, ranges.get(i).f0),
                            new BsonDocument(GRP_FIELD, grp0).append(ID_FIELD, ranges.get(i).f1),
                            GRP_ID_INDEX_HINT));
        }
        return results;
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
                            TEST_NS.getFullName() + "_" + i,
                            TEST_NS.getDatabaseName(),
                            TEST_NS.getCollectionName(),
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

    private static final List<MongoScanSourceSplit> FILTERED_SINGLE_SPLIT =
            Collections.singletonList(
                    new MongoScanSourceSplit(
                            FILTER_TEST_NS.getFullName(),
                            FILTER_TEST_NS.getDatabaseName(),
                            FILTER_TEST_NS.getCollectionName(),
                            new BsonDocument(GRP_FIELD, new BsonInt32(0))
                                    .append(ID_FIELD, BSON_MIN_KEY),
                            new BsonDocument(GRP_FIELD, new BsonInt32(0))
                                    .append(ID_FIELD, BSON_MAX_KEY),
                            GRP_ID_INDEX_HINT));
}
