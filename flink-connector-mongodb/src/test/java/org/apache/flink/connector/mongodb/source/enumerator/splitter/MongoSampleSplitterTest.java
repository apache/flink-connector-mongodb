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
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MAX_BOUNDARY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MIN_BOUNDARY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_FIELD;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link MongoSampleSplitter}. */
class MongoSampleSplitterTest {

    private static final MongoNamespace TEST_NS = new MongoNamespace("test.test");

    @Test
    void testSplitEmptyCollection() {
        MongoSplitContext splitContext =
                new MongoSplitContext(
                        MongoReadOptions.builder().build(), null, TEST_NS, false, 0, 0, 0);

        assertSingleSplit(
                new ArrayList<>(
                        MongoSampleSplitter.split(splitContext, (i1, i2) -> new ArrayList<>())));
    }

    @Test
    void testLargerSizedPartitions() {
        long totalNumDocuments = 10000L;

        MemorySize avgObjSize = new MemorySize(160L);
        MemorySize totalStorageSize = avgObjSize.multiply(totalNumDocuments);

        MongoSplitContext splitContext =
                new MongoSplitContext(
                        MongoReadOptions.builder().setPartitionSize(totalStorageSize).build(),
                        null,
                        TEST_NS,
                        false,
                        totalNumDocuments,
                        totalStorageSize.getBytes(),
                        avgObjSize.getBytes());

        assertSingleSplit(
                new ArrayList<>(
                        MongoSampleSplitter.split(splitContext, (i1, i2) -> new ArrayList<>())));
    }

    @Test
    void testNumberOfSampleCalculation() {
        long totalNumDocuments = 100L;
        int numPartitions = 10;

        MemorySize avgObjSize = MemorySize.ofMebiBytes(10);
        MemorySize totalStorageSize = avgObjSize.multiply(totalNumDocuments);
        MemorySize partitionSize = totalStorageSize.divide(numPartitions);

        int samplesPerPartition = 2;
        int numExpectedSamples = samplesPerPartition * numPartitions - 1;

        MongoSplitContext splitContext =
                new MongoSplitContext(
                        MongoReadOptions.builder()
                                .setPartitionSize(partitionSize)
                                .setSamplesPerPartition(2)
                                .build(),
                        null,
                        TEST_NS,
                        false,
                        totalNumDocuments,
                        totalStorageSize.getBytes(),
                        avgObjSize.getBytes());

        MongoSampleSplitter.split(
                splitContext,
                (ignored, numRequestedSamples) -> {
                    assertThat(numRequestedSamples).isEqualTo(numExpectedSamples);
                    return createSamples(numRequestedSamples);
                });
    }

    @Test
    void testSampleMerging() {
        final int numPartitions = 3;
        final int samplesPerPartition = 2;
        final List<BsonDocument> samples = createSamples(numPartitions * samplesPerPartition - 1);

        List<MongoScanSourceSplit> splits =
                MongoSampleSplitter.createSplits(samples, samplesPerPartition, TEST_NS);

        // Samples:      0 1 2 3 4
        // Bounds:     -           +
        // Partitions: |-|-|-|-|-|-|
        // Splits:     |---|---|---|
        assertThat(splits).hasSize(numPartitions);
        assertThat(splits.get(0))
                .satisfies(
                        split -> {
                            assertThat(split.getMin()).isEqualTo(BSON_MIN_BOUNDARY);
                            assertThat(split.getMax()).isEqualTo(samples.get(1));
                        });
        assertThat(splits.get(1))
                .satisfies(
                        split -> {
                            assertThat(split.getMin()).isEqualTo(samples.get(1));
                            assertThat(split.getMax()).isEqualTo(samples.get(3));
                        });
        assertThat(splits.get(2))
                .satisfies(
                        split -> {
                            assertThat(split.getMin()).isEqualTo(samples.get(3));
                            assertThat(split.getMax()).isEqualTo(BSON_MAX_BOUNDARY);
                        });
    }

    private static List<BsonDocument> createSamples(int samplesCount) {
        List<BsonDocument> samples = new ArrayList<>(samplesCount);
        for (int i = 0; i < samplesCount; i++) {
            samples.add(new BsonDocument(ID_FIELD, new BsonInt32(i)));
        }
        return samples;
    }

    private static void assertSingleSplit(List<MongoScanSourceSplit> splits) {
        assertThat(splits.size()).isEqualTo(1);
        assertThat(splits.get(0).getMin()).isEqualTo(BSON_MIN_BOUNDARY);
        assertThat(splits.get(0).getMax()).isEqualTo(BSON_MAX_BOUNDARY);
    }
}
