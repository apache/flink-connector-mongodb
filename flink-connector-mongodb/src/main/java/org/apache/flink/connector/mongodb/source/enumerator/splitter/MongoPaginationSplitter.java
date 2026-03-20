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
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Projections;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.AND_OPERATOR;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MAX_KEY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MIN_KEY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.EQ_OPERATOR;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_HINT;

/** Mongo Splitter that splits MongoDB collection evenly by record counts. */
@Internal
public class MongoPaginationSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(MongoPaginationSplitter.class);

    public static Collection<MongoScanSourceSplit> split(MongoSplitContext splitContext) {
        MongoReadOptions readOptions = splitContext.getReadOptions();
        MongoNamespace namespace = splitContext.getMongoNamespace();
        BsonDocument filter = readOptions.getFilter();

        // Determine best supporting index if filter provided, otherwise _id
        // Only equality fields qualify
        BsonDocument indexHint = findBestSupportingIndex(splitContext.getMongoCollection(), filter);

        // If filter present, only include indexed fields for query to build the splits
        // This prevents collection scan in favor of index scan for fast performance
        filter = filterMatchingIndexKeys(indexHint, filter);

        // If partition record size isn't present, we'll use the partition size option and average
        // object size to calculate number of records in each partitioned split.
        Integer partitionRecordSize = readOptions.getPartitionRecordSize();
        if (partitionRecordSize == null) {
            long avgObjSizeInBytes = splitContext.getAvgObjSize();
            if (avgObjSizeInBytes == 0) {
                LOG.info(
                        "{} seems to be an empty collection, Returning a single partition.",
                        namespace);

                return MongoSingleSplitter.split(
                        splitContext,
                        createIndexBound(indexHint, filter, BSON_MIN_KEY, BSON_MIN_KEY),
                        createIndexBound(indexHint, filter, BSON_MAX_KEY, BSON_MAX_KEY),
                        indexHint);
            }

            // At least one record per split, a skip of 0 would never advance the boundary
            partitionRecordSize =
                    Math.max(
                            1,
                            Math.toIntExact(
                                    readOptions.getPartitionSize().getBytes() / avgObjSizeInBytes));
        }

        LOG.info(
                "Determining pagination split for '{}' with {} records per split: index={} filter={}",
                namespace,
                partitionRecordSize,
                indexHint,
                filter);

        long startNanos = System.nanoTime();

        List<BsonValue> splitBoundaries =
                collectSplitBoundaries(
                        splitContext.getMongoCollection(), filter, indexHint, partitionRecordSize);

        List<MongoScanSourceSplit> splits = new ArrayList<>(splitBoundaries.size() + 1);
        BsonValue lowerBound = BSON_MIN_KEY;

        for (int splitNum = 0; splitNum < splitBoundaries.size(); splitNum++) {
            BsonValue upperBound = splitBoundaries.get(splitNum);

            splits.add(
                    new MongoScanSourceSplit(
                            String.format("%s_%d", namespace, splitNum),
                            namespace.getDatabaseName(),
                            namespace.getCollectionName(),
                            createIndexBound(indexHint, filter, lowerBound, BSON_MIN_KEY),
                            createIndexBound(indexHint, filter, upperBound, BSON_MAX_KEY),
                            indexHint));

            lowerBound = upperBound;
        }

        // Final tail split
        // In the case of no split boundaries, then this will be from min->max (a single split)
        splits.add(
                new MongoScanSourceSplit(
                        namespace + (splitBoundaries.isEmpty() ? "" : "_" + splitBoundaries.size()),
                        namespace.getDatabaseName(),
                        namespace.getCollectionName(),
                        createIndexBound(indexHint, filter, lowerBound, BSON_MIN_KEY),
                        createIndexBound(indexHint, filter, BSON_MAX_KEY, BSON_MAX_KEY),
                        indexHint));

        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        LOG.info(
                "Completed pagination split for '{}' with {} splits in {}ms: index={} filter={}",
                namespace,
                splits.size(),
                durationMs,
                indexHint,
                filter);

        return splits;
    }

    private static List<BsonValue> collectSplitBoundaries(
            MongoCollection<BsonDocument> collection,
            @Nullable BsonDocument filter,
            BsonDocument indexHint,
            int partitionRecordSize) {

        List<BsonValue> boundaries = new ArrayList<>();
        BsonDocument maxBound = createIndexBound(indexHint, filter, BSON_MAX_KEY, BSON_MAX_KEY);
        BsonValue lastBoundary = BSON_MIN_KEY;

        // Per-boundary index walk: each query positions at the previous boundary using min/max
        // hints, then skips `partitionRecordSize` index entries and returns one _id as the next
        // boundary. On the first iteration `min` starts at the beginning of the index range; on
        // subsequent iterations it re-enters the index exactly at the previous boundary so each
        // skip covers exactly one split's worth of entries. The explicit sort on the index pattern
        // guarantees index order when results are merged from multiple shards (served by the index
        // so no blocking sort). Note: _id: -1 index order is not supported.
        while (true) {
            BsonDocument minBound = createIndexBound(indexHint, filter, lastBoundary, BSON_MIN_KEY);

            try (MongoCursor<BsonDocument> cursor =
                    collection
                            .find()
                            .projection(Projections.include(ID_FIELD))
                            .hint(indexHint)
                            .sort(indexHint)
                            .min(minBound)
                            .max(maxBound)
                            .skip(partitionRecordSize)
                            .limit(1)
                            .iterator()) {

                if (!cursor.hasNext()) {
                    break;
                }

                BsonValue boundary = cursor.next().get(ID_FIELD);
                if (boundary == null || boundary.equals(lastBoundary)) {
                    // Guard against an infinite loop if the boundary fails to advance
                    throw new IllegalStateException(
                            String.format(
                                    "Pagination split boundary did not advance past '%s' for index '%s'",
                                    lastBoundary, indexHint));
                }
                boundaries.add(boundary);
                lastBoundary = boundary;
            }
        }

        return boundaries;
    }

    /**
     * Determine the index hint to use by determining "the best" index to use for the filter (if
     * provided).
     */
    @VisibleForTesting
    static BsonDocument findBestSupportingIndex(
            MongoCollection<BsonDocument> collection, @Nullable BsonDocument filter) {

        if (filter == null) {
            return ID_HINT;
        }

        Set<String> filterKeys = extractEqualityFields(filter);
        if (filterKeys.isEmpty()) {
            return ID_HINT;
        }

        Document bestIndex = null;
        int bestScore = -1;

        try (MongoCursor<Document> cursor = collection.listIndexes().iterator()) {
            while (cursor.hasNext()) {
                Document indexDoc = cursor.next();

                if (!isSupportedIndexType(indexDoc)) {
                    continue;
                }

                // Key contains the actual index document
                Document keyDoc = indexDoc.get("key", Document.class);
                if (keyDoc == null) {
                    continue;
                }

                int score = indexSupportsPaginationSplitter(keyDoc, filterKeys);

                if (score > bestScore) {
                    bestScore = score;
                    bestIndex = keyDoc;
                }
            }
        }

        if (bestIndex == null) {
            return ID_HINT;
        }

        return bestIndex.toBsonDocument();
    }

    /**
     * Hidden indexes can't be hinted. Partial indexes may not contain every document matching the
     * filter, and collated indexes reject min/max bounds from a query without a matching collation.
     */
    private static boolean isSupportedIndexType(Document indexDoc) {
        return !indexDoc.getBoolean("hidden", false)
                && !indexDoc.containsKey("partialFilterExpression")
                && !indexDoc.containsKey("collation");
    }

    /** Return an index score where greater the score the better the index is for the filter. */
    private static int indexSupportsPaginationSplitter(Document keyDoc, Set<String> filterKeys) {

        int matchedFields = 0;

        for (Map.Entry<String, Object> entry : keyDoc.entrySet()) {
            String key = entry.getKey();
            Object direction = entry.getValue();

            // Once _id is found in the index, then it is a qualifying index
            // The more matched fields, the better the index!
            // Boundaries are walked in ascending _id order, thus _id must be ascending
            if (ID_FIELD.equals(key)) {
                return isDirection(direction, 1) ? matchedFields : -1;
            }

            // Not a qualifying index, also excludes hashed/text/geo key types whose index values
            // differ from the filter values
            if (!filterKeys.contains(key)
                    || !(isDirection(direction, 1) || isDirection(direction, -1))) {
                return -1;
            }

            matchedFields++;
        }

        return -1;
    }

    private static boolean isDirection(Object direction, int expected) {
        return direction instanceof Number && ((Number) direction).doubleValue() == expected;
    }

    /** Determine all equality keys, flattening any wrapped $and expressions. */
    @VisibleForTesting
    static Set<String> extractEqualityFields(BsonDocument filter) {
        return new HashSet<>(flattenEqualityPredicates(filter).keySet());
    }

    /**
     * Collects equality predicates from the top level and from nested $and (recursively) into one
     * document, with $eq unwrapped to its literal value. The first occurrence of each field name
     * wins.
     */
    private static BsonDocument flattenEqualityPredicates(BsonDocument filter) {
        BsonDocument out = new BsonDocument();
        flattenEqualityPredicates(filter, out);
        return out;
    }

    private static void flattenEqualityPredicates(BsonDocument filter, BsonDocument out) {
        for (Map.Entry<String, BsonValue> entry : filter.entrySet()) {
            String key = entry.getKey();
            BsonValue value = entry.getValue();

            if (AND_OPERATOR.equals(key) && value.isArray()) {
                for (BsonValue child : value.asArray()) {
                    if (child.isDocument()) {
                        flattenEqualityPredicates(child.asDocument(), out);
                    }
                }
                continue;
            }

            // Other top level operators ($or, $nor, $expr, $comment, ...) are not field predicates
            if (key.startsWith("$")) {
                continue;
            }

            BsonValue literal = equalityLiteral(value);
            if (literal != null && !out.containsKey(key)) {
                out.append(key, literal);
            }
        }
    }

    /**
     * Only equality fields qualify to be used in min/max boundary. Returns the literal value usable
     * as an index bound, or null if the predicate isn't an equality on a single index key value.
     */
    private static @Nullable BsonValue equalityLiteral(BsonValue value) {
        BsonValue literal = value;

        if (value.isDocument()) {
            BsonDocument doc = value.asDocument();

            // { a: { $eq: 1 } }
            if (doc.size() != 1 || !doc.containsKey(EQ_OPERATOR)) {
                return null;
            }
            literal = doc.get(EQ_OPERATOR);
        }

        // Regex is a pattern match, and arrays/documents may match multiple index keys
        if (literal.isRegularExpression() || literal.isArray() || literal.isDocument()) {
            return null;
        }

        return literal;
    }

    /**
     * Include only equality operations present in the index prefix before _id. Handle when criteria
     * is within an inner $and.
     */
    @VisibleForTesting
    static BsonDocument filterMatchingIndexKeys(
            BsonDocument indexHint, @Nullable BsonDocument filter) {

        if (filter == null) {
            return null;
        }

        BsonDocument flat = flattenEqualityPredicates(filter);
        BsonDocument doc = new BsonDocument();
        for (String indexKey : indexHint.keySet()) {
            if (ID_FIELD.equals(indexKey)) {
                break;
            }
            if (flat.containsKey(indexKey)) {
                doc.append(indexKey, flat.get(indexKey));
            }
        }
        return doc;
    }

    /**
     * Create index min/max boundaries. Keys before _id must be present in the filter; _id gets the
     * split boundary value; any additional index keys after _id are considered wildcard (either:
     * BSON_MIN_KEY/BSON_MAX_KEY).
     */
    @VisibleForTesting
    static BsonDocument createIndexBound(
            BsonDocument indexHint,
            @Nullable BsonDocument filter,
            BsonValue idValue,
            BsonValue otherValues) {

        BsonDocument doc = new BsonDocument();

        if (filter == null) {
            doc.append(ID_FIELD, idValue);
            return doc;
        }

        boolean keyFollowsId = false;

        // Backed by linkedHashMap thus order preserved
        for (String key : indexHint.keySet()) {
            if (ID_FIELD.equals(key)) {
                doc.append(ID_FIELD, idValue);
                keyFollowsId = true;
            } else if (keyFollowsId) {
                // Ignore index keys following _id and treat them as wildcard
                doc.append(key, otherValues);
            } else if (filter.containsKey(key)) {
                doc.append(key, filter.get(key));
            } else {
                // This _should_ never happen
                // Previously validated filtered keys are within the index < _id
                throw new IllegalStateException("Filter must contain indexed key: " + key);
            }
        }

        return doc;
    }
}
