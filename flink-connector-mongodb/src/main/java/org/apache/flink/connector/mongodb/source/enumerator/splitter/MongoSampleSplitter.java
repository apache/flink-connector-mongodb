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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.mongodb.source.config.MongoReadOptions;
import org.apache.flink.connector.mongodb.source.split.MongoScanSourceSplit;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MAX_BOUNDARY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MIN_BOUNDARY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_HINT;

/**
 * Sample Partitioner
 *
 * <p>Samples the collection to generate partitions.
 *
 * <p>Uses the average document size to split the collection into average sized chunks
 *
 * <p>The partitioner samples the collection, projects and sorts by the partition fields. Then uses
 * every {@code samplesPerPartition} as the value to use to calculate the partition boundaries.
 *
 * <ul>
 *   <li>scan.partition.size: The average size (MB) for each partition. Note: Uses the average
 *       document size to determine the number of documents per partition so may not be even.
 *       Defaults to: 64mb.
 *   <li>scan.partition.samples: The number of samples to take per partition. Defaults to: 10. The
 *       total number of samples taken is calculated as: {@code samples per partition * (count of
 *       documents / number of documents per partition)}.
 * </ul>
 */
@Internal
public class MongoSampleSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(MongoSampleSplitter.class);

    public static Collection<MongoScanSourceSplit> split(MongoSplitContext splitContext) {
        return split(splitContext, new DefaultMongoSampler());
    }

    @VisibleForTesting
    static Collection<MongoScanSourceSplit> split(
            MongoSplitContext splitContext,
            BiFunction<MongoSplitContext, Integer, List<BsonDocument>> sampler) {
        MongoReadOptions readOptions = splitContext.getReadOptions();
        MongoNamespace namespace = splitContext.getMongoNamespace();

        long totalNumDocuments = splitContext.getCount();
        long partitionSizeInBytes = readOptions.getPartitionSize().getBytes();
        int samplesPerPartition = readOptions.getSamplesPerPartition();

        long avgObjSizeInBytes = splitContext.getAvgObjSize();
        if (avgObjSizeInBytes == 0L) {
            LOG.info(
                    "{} seems to be an empty collection, Returning a single partition.", namespace);
            return MongoSingleSplitter.split(splitContext);
        }

        long numDocumentsPerPartition = partitionSizeInBytes / avgObjSizeInBytes;
        if (numDocumentsPerPartition >= totalNumDocuments) {
            LOG.info(
                    "Fewer documents ({}) than the number of documents per partition ({}), Returning a single partition.",
                    totalNumDocuments,
                    numDocumentsPerPartition);
            return MongoSingleSplitter.split(splitContext);
        }

        int numberOfPartitions =
                (int) Math.ceil(totalNumDocuments * 1.0d / numDocumentsPerPartition);
        // N samples divide the data into N + 1 partitions
        int numberOfSamples = samplesPerPartition * numberOfPartitions - 1;

        List<BsonDocument> samples = sampler.apply(splitContext, numberOfSamples);

        return createSplits(samples, samplesPerPartition, namespace);
    }

    @VisibleForTesting
    static List<MongoScanSourceSplit> createSplits(
            List<BsonDocument> samples, int samplesPerPartition, MongoNamespace namespace) {
        samples.add(BSON_MAX_BOUNDARY);

        List<MongoScanSourceSplit> sourceSplits = new ArrayList<>();
        BsonDocument partitionStart = BSON_MIN_BOUNDARY;
        int splitNum = 0;
        for (int i = samplesPerPartition - 1; i < samples.size(); i += samplesPerPartition) {
            sourceSplits.add(createSplit(namespace, splitNum++, partitionStart, samples.get(i)));
            partitionStart = samples.get(i);
        }

        return sourceSplits;
    }

    private static class DefaultMongoSampler
            implements BiFunction<MongoSplitContext, Integer, List<BsonDocument>> {

        @Override
        public List<BsonDocument> apply(MongoSplitContext splitContext, Integer numberOfSamples) {
            return splitContext
                    .getMongoCollection()
                    .aggregate(
                            Arrays.asList(
                                    Aggregates.sample(numberOfSamples),
                                    Aggregates.project(Projections.include(ID_FIELD)),
                                    Aggregates.sort(Sorts.ascending(ID_FIELD))))
                    .allowDiskUse(true)
                    .into(new ArrayList<>());
        }
    }

    private static MongoScanSourceSplit createSplit(
            MongoNamespace ns, int index, BsonDocument min, BsonDocument max) {
        return new MongoScanSourceSplit(
                String.format("%s_%d", ns, index),
                ns.getDatabaseName(),
                ns.getCollectionName(),
                min,
                max,
                ID_HINT);
    }
}
