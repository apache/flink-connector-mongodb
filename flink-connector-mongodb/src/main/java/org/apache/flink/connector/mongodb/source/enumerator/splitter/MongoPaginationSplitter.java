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
import org.apache.flink.connector.mongodb.source.config.MongoReadOptions;
import org.apache.flink.connector.mongodb.source.split.MongoScanSourceSplit;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MAX_BOUNDARY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MIN_BOUNDARY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_HINT;

/** Mongo Splitter that splits MongoDB collection evenly by record counts. */
@Internal
public class MongoPaginationSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(MongoPaginationSplitter.class);

    public static Collection<MongoScanSourceSplit> split(MongoSplitContext splitContext) {
        MongoReadOptions readOptions = splitContext.getReadOptions();
        MongoNamespace namespace = splitContext.getMongoNamespace();

        // If partition record size isn't present, we'll use the partition size option and average
        // object size to calculate number of records in each partitioned split.
        Integer partitionRecordSize = readOptions.getPartitionRecordSize();
        if (partitionRecordSize == null) {
            long avgObjSizeInBytes = splitContext.getAvgObjSize();
            if (avgObjSizeInBytes == 0) {
                LOG.info(
                        "{} seems to be an empty collection, Returning a single partition.",
                        namespace);
                return MongoSingleSplitter.split(splitContext);
            }

            partitionRecordSize =
                    Math.toIntExact(readOptions.getPartitionSize().getBytes() / avgObjSizeInBytes);
        }

        long totalNumOfDocuments = splitContext.getCount();

        if (partitionRecordSize >= totalNumOfDocuments) {
            LOG.info(
                    "Fewer documents ({}) than the number of documents per partition ({}), Returning a single partition.",
                    totalNumOfDocuments,
                    partitionRecordSize);
            return MongoSingleSplitter.split(splitContext);
        }

        int numberOfPartitions =
                (int) (Math.ceil(totalNumOfDocuments / (double) partitionRecordSize));

        BsonDocument lastUpperBound = null;
        List<MongoScanSourceSplit> paginatedSplits = new ArrayList<>();

        for (int splitNum = 0; splitNum < numberOfPartitions; splitNum++) {
            List<Bson> pipeline = new ArrayList<>();

            pipeline.add(Aggregates.project(Projections.include(ID_FIELD)));
            pipeline.add(Aggregates.project(Sorts.ascending(ID_FIELD)));

            // We don't have to set the upper bounds limit if we're generating the first split.
            if (lastUpperBound != null) {
                BsonDocument matchFilter = new BsonDocument();
                if (lastUpperBound.containsKey(ID_FIELD)) {
                    matchFilter.put(
                            ID_FIELD, new BsonDocument("$gte", lastUpperBound.get(ID_FIELD)));
                }
                pipeline.add(Aggregates.match(matchFilter));
            }
            pipeline.add(Aggregates.skip(partitionRecordSize));
            pipeline.add(Aggregates.limit(1));

            BsonDocument currentUpperBound =
                    splitContext
                            .getMongoCollection()
                            .aggregate(pipeline)
                            .allowDiskUse(true)
                            .first();

            paginatedSplits.add(
                    new MongoScanSourceSplit(
                            String.format("%s_%d", namespace, splitNum),
                            namespace.getDatabaseName(),
                            namespace.getCollectionName(),
                            lastUpperBound != null ? lastUpperBound : BSON_MIN_BOUNDARY,
                            currentUpperBound != null ? currentUpperBound : BSON_MAX_BOUNDARY,
                            ID_HINT));

            if (currentUpperBound == null) {
                break;
            }
            lastUpperBound = currentUpperBound;
        }

        return paginatedSplits;
    }
}
