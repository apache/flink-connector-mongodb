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

package org.apache.flink.connector.mongodb.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.mongodb.table.converter.RowDataToBsonConverters;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.function.SerializableFunction;

import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/** An extractor for a MongoDB shard keys from a {@link RowData}. */
@Internal
public class MongoShardKeysExtractor implements SerializableFunction<RowData, BsonDocument> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(MongoShardKeysExtractor.class);

    private static final BsonDocument EMPTY_DOCUMENT = new BsonDocument();

    private final SerializableFunction<Object, BsonValue> shardKeysConverter;

    private final RowData.FieldGetter shardKeysGetter;

    private MongoShardKeysExtractor(LogicalType shardKeysType, int[] shardKeysIndexes) {
        this.shardKeysConverter = RowDataToBsonConverters.createFieldDataConverter(shardKeysType);
        this.shardKeysGetter =
                rowData -> ProjectedRowData.from(shardKeysIndexes).replaceRow(rowData);
    }

    @Override
    public BsonDocument apply(RowData rowData) {
        BsonDocument shardKeysDoc =
                Optional.ofNullable(shardKeysGetter.getFieldOrNull(rowData))
                        .map(shardKeys -> (BsonDocument) shardKeysConverter.apply(shardKeys))
                        .orElse(EMPTY_DOCUMENT);

        shardKeysDoc
                .entrySet()
                .forEach(
                        entry -> {
                            if (entry.getValue().isString()) {
                                String keyString = entry.getValue().asString().getValue();
                                // Try to restore MongoDB's ObjectId from string.
                                if (ObjectId.isValid(keyString)) {
                                    entry.setValue(new BsonObjectId(new ObjectId(keyString)));
                                }
                            }
                        });

        return shardKeysDoc;
    }

    public static SerializableFunction<RowData, BsonDocument> createShardKeyExtractor(
            ResolvedSchema resolvedSchema, String[] shardKeys) {
        // no shard keys are declared.
        if (shardKeys.length == 0) {
            return new NoOpShardKeysExtractor();
        }

        int[] shardKeysIndexes = getShardKeysIndexes(resolvedSchema.getColumnNames(), shardKeys);
        DataType physicalRowDataType = resolvedSchema.toPhysicalRowDataType();
        DataType shardKeysType = Projection.of(shardKeysIndexes).project(physicalRowDataType);

        MongoShardKeysExtractor shardKeysExtractor =
                new MongoShardKeysExtractor(shardKeysType.getLogicalType(), shardKeysIndexes);

        LOG.info("Shard keys extractor created, shard keys: {}", Arrays.toString(shardKeys));
        return shardKeysExtractor;
    }

    private static int[] getShardKeysIndexes(List<String> columnNames, String[] shardKeys) {
        return Arrays.stream(shardKeys).mapToInt(columnNames::indexOf).toArray();
    }

    /**
     * It behaves as no-op extractor when no shard keys are declared. We use static class instead of
     * lambda because the maven shade plugin cannot relocate classes in SerializedLambdas
     * (MSHADE-260).
     */
    private static class NoOpShardKeysExtractor
            implements SerializableFunction<RowData, BsonDocument> {

        private static final long serialVersionUID = 1L;

        @Override
        public BsonDocument apply(RowData rowData) {
            return EMPTY_DOCUMENT;
        }
    }
}
