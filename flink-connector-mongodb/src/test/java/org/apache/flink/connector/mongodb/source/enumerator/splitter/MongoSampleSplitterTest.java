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

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.mongodb.source.config.MongoReadOptions;
import org.apache.flink.connector.mongodb.source.split.MongoScanSourceSplit;

import com.mongodb.MongoNamespace;
import org.apache.commons.lang3.RandomUtils;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MAX_KEY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MIN_KEY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_FIELD;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link MongoSampleSplitter}. */
class MongoSampleSplitterTest {

    private static final MongoNamespace TEST_NS = new MongoNamespace("test.test");
    private static final BsonDocument MIN = new BsonDocument(ID_FIELD, BSON_MIN_KEY);
    private static final BsonDocument MAX = new BsonDocument(ID_FIELD, BSON_MAX_KEY);
    private static final int STEP_SIZE = 101;

    @Test
    void testSplitEmptyCollection() {
        MongoSampleSplitter sampleSplitter = new MongoSampleSplitter((i1, i2) -> new ArrayList<>());

        MongoSplitContext splitContext =
                new MongoSplitContext(
                        MongoReadOptions.builder().build(), null, TEST_NS, false, 0, 0, 0);

        assertSingleSplit(new ArrayList<>(sampleSplitter.split(splitContext)));
    }

    @Test
    void testLargerSizedPartitions() {
        MongoSampleSplitter sampleSplitter = new MongoSampleSplitter((i1, i2) -> new ArrayList<>());

        long totalNumDocuments = 10000L;
        long avgObjSizeInBytes = 160L;
        long totalStorageSize = totalNumDocuments * avgObjSizeInBytes;

        long partitionSizeInBytes = totalNumDocuments * avgObjSizeInBytes;

        MongoSplitContext splitContext =
                new MongoSplitContext(
                        MongoReadOptions.builder()
                                .setPartitionSize(new MemorySize(partitionSizeInBytes))
                                .build(),
                        null,
                        TEST_NS,
                        false,
                        totalNumDocuments,
                        totalStorageSize,
                        avgObjSizeInBytes);

        assertSingleSplit(new ArrayList<>(sampleSplitter.split(splitContext)));
    }

    @Test
    void testSplitBoundaries() {
        long totalNumDocuments = RandomUtils.nextLong(1000000L, 2000000L);
        long avgObjSizeInBytes = 260L;
        int samplesPerPartition = 11;
        MemorySize partitionSize = MemorySize.parse("2mb");
        long numDocumentsPerPartition = partitionSize.getBytes() / avgObjSizeInBytes;
        long totalStorageSize = totalNumDocuments * avgObjSizeInBytes;

        int numberOfPartitions =
                (int) Math.ceil(totalNumDocuments * 1.0d / numDocumentsPerPartition);
        int numberOfSamples = samplesPerPartition * numberOfPartitions;

        List<BsonDocument> samples = createSamples(numberOfSamples);

        MongoSampleSplitter sampleSplitter = new MongoSampleSplitter((i1, i2) -> samples);

        MongoSplitContext splitContext =
                new MongoSplitContext(
                        MongoReadOptions.builder()
                                .setSamplesPerPartition(samplesPerPartition)
                                .setPartitionSize(partitionSize)
                                .build(),
                        null,
                        TEST_NS,
                        false,
                        totalNumDocuments,
                        totalStorageSize,
                        avgObjSizeInBytes);

        List<MongoScanSourceSplit> splits = new ArrayList<>(sampleSplitter.split(splitContext));

        // Assert boundaries can include the entire collection.
        assertThat(splits).hasSize(numberOfPartitions);
        assertThat(splits.get(0).getMin()).isEqualTo(MIN);
        assertThat(splits.get(splits.size() - 1).getMax()).isEqualTo(MAX);

        MongoScanSourceSplit previous = splits.get(0);
        for (int i = 1; i < splits.size(); i++) {
            assertThat(previous.getMax()).isEqualTo(splits.get(i).getMin());
            previous = splits.get(i);
        }
    }

    private static List<BsonDocument> createSamples(int samplesCount) {
        List<BsonDocument> samples = new ArrayList<>(samplesCount);
        for (int i = 0; i < samplesCount; i++) {
            samples.add(new BsonDocument(ID_FIELD, new BsonInt32(i * STEP_SIZE)));
        }
        return samples;
    }

    private static void assertSingleSplit(List<MongoScanSourceSplit> splits) {
        assertThat(splits.size()).isEqualTo(1);
        assertThat(splits.get(0).getMin()).isEqualTo(MIN);
        assertThat(splits.get(0).getMax()).isEqualTo(MAX);
    }
}
