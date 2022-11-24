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

package org.apache.flink.connector.mongodb.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.mongodb.common.utils.MongoValidationUtils;
import org.apache.flink.connector.mongodb.table.converter.RowDataToBsonConverters;
import org.apache.flink.connector.mongodb.table.converter.RowDataToBsonConverters.RowDataToBsonConverter;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.function.SerializableFunction;

import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

import java.util.Optional;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_FIELD;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** An extractor for a MongoDB key from a {@link RowData}. */
@Internal
public class MongoKeyExtractor implements SerializableFunction<RowData, BsonValue> {

    public static final String RESERVED_ID = ID_FIELD;

    private static final AppendOnlyKeyExtractor APPEND_ONLY_KEY_EXTRACTOR =
            new AppendOnlyKeyExtractor();

    private final int[] primaryKeyIndexes;

    private final RowDataToBsonConverter primaryKeyConverter;

    private final FieldGetter primaryKeyGetter;

    private MongoKeyExtractor(LogicalType primaryKeyType, int[] primaryKeyIndexes) {
        this.primaryKeyIndexes = primaryKeyIndexes;
        this.primaryKeyConverter = RowDataToBsonConverters.createNullableConverter(primaryKeyType);
        if (isCompoundPrimaryKey(primaryKeyIndexes)) {
            this.primaryKeyGetter =
                    rowData -> ProjectedRowData.from(primaryKeyIndexes).replaceRow(rowData);
        } else {
            this.primaryKeyGetter = RowData.createFieldGetter(primaryKeyType, primaryKeyIndexes[0]);
        }
    }

    @Override
    public BsonValue apply(RowData rowData) {
        Object rowKeyValue = primaryKeyGetter.getFieldOrNull(rowData);
        checkNotNull(rowKeyValue, "Primary key value is null of RowData: " + rowData);
        BsonValue keyValue = primaryKeyConverter.convert(rowKeyValue);
        if (!isCompoundPrimaryKey(primaryKeyIndexes) && keyValue.isString()) {
            String keyString = keyValue.asString().getValue();
            // Try to restore MongoDB's ObjectId from string.
            if (ObjectId.isValid(keyString)) {
                keyValue = new BsonObjectId(new ObjectId(keyString));
            }
        }
        return keyValue;
    }

    public static SerializableFunction<RowData, BsonValue> createKeyExtractor(
            ResolvedSchema resolvedSchema) {

        Optional<UniqueConstraint> primaryKey = resolvedSchema.getPrimaryKey();
        int[] primaryKeyIndexes = resolvedSchema.getPrimaryKeyIndexes();
        Optional<Column> reservedId = resolvedSchema.getColumn(RESERVED_ID);

        // Primary key is not declared and reserved _id is not present.
        if (!primaryKey.isPresent() && !reservedId.isPresent()) {
            return APPEND_ONLY_KEY_EXTRACTOR;
        }

        if (reservedId.isPresent()) {
            // Ambiguous keys being used due to the presence of an _id field.
            if (!primaryKey.isPresent()
                    || isCompoundPrimaryKey(primaryKeyIndexes)
                    || !primaryKeyContainsReservedId(primaryKey.get())) {
                throw new IllegalArgumentException(
                        "Ambiguous keys being used due to the presence of an _id field.");
            }
        }

        DataType primaryKeyType;
        if (isCompoundPrimaryKey(primaryKeyIndexes)) {
            DataType physicalRowDataType = resolvedSchema.toPhysicalRowDataType();
            primaryKeyType = Projection.of(primaryKeyIndexes).project(physicalRowDataType);
        } else {
            int primaryKeyIndex = primaryKeyIndexes[0];
            Optional<Column> column = resolvedSchema.getColumn(primaryKeyIndex);
            if (!column.isPresent()) {
                throw new IllegalStateException(
                        String.format(
                                "No primary key column found with index '%s'.", primaryKeyIndex));
            }
            primaryKeyType = column.get().getDataType();
        }

        MongoValidationUtils.validatePrimaryKey(primaryKeyType);

        return new MongoKeyExtractor(primaryKeyType.getLogicalType(), primaryKeyIndexes);
    }

    private static boolean isCompoundPrimaryKey(int[] primaryKeyIndexes) {
        return primaryKeyIndexes.length > 1;
    }

    private static boolean primaryKeyContainsReservedId(UniqueConstraint primaryKey) {
        return primaryKey.getColumns().contains(RESERVED_ID);
    }

    /**
     * It behaves as append-only when no primary key is declared and reserved _id is not present. We
     * use static class instead of lambda for a reason here. It is necessary because the maven shade
     * plugin cannot relocate classes in SerializedLambdas (MSHADE-260).
     */
    private static class AppendOnlyKeyExtractor
            implements SerializableFunction<RowData, BsonValue> {
        private static final long serialVersionUID = 1L;

        @Override
        public BsonValue apply(RowData rowData) {
            return null;
        }
    }
}
