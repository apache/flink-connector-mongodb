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

package org.apache.flink.connector.mongodb.common.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.mongodb.table.MongoKeyExtractor;
import org.apache.flink.connector.mongodb.table.converter.RowDataToBsonConverters;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.bson.BsonType;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Utility methods for validating MongoDB properties. */
@Internal
public class MongoValidationUtils {
    private static final Set<LogicalTypeRoot> ALLOWED_PRIMARY_KEY_TYPES =
            EnumSet.of(
                    LogicalTypeRoot.CHAR,
                    LogicalTypeRoot.VARCHAR,
                    LogicalTypeRoot.BOOLEAN,
                    LogicalTypeRoot.DECIMAL,
                    LogicalTypeRoot.TINYINT,
                    LogicalTypeRoot.SMALLINT,
                    LogicalTypeRoot.INTEGER,
                    LogicalTypeRoot.BIGINT,
                    LogicalTypeRoot.FLOAT,
                    LogicalTypeRoot.DOUBLE,
                    LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                    LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                    LogicalTypeRoot.INTERVAL_YEAR_MONTH,
                    LogicalTypeRoot.INTERVAL_DAY_TIME);

    /**
     * Checks that the table does not have a primary key defined on illegal types. In MongoDB the
     * primary key is used to calculate the MongoDB document _id, which may be of any {@link
     * BsonType} other than a {@link BsonType#ARRAY}. Its value must be unique and immutable in the
     * collection.
     *
     * <p>MongoDB creates a unique index on the _id field during the creation of a collection. There
     * are also some constraints on the primary key index. For more detailed introduction, you can
     * refer to <a
     * href="https://www.mongodb.com/docs/manual/reference/limits/#mongodb-limit-Index-Key-Limit">
     * Index Key Limit</a>.
     *
     * <ul>
     *   <li>Before MongoDB 4.2, the total size of an index entry, which can include structural
     *       overhead depending on the BSON type, must be less than 1024 bytes.
     *   <li>Starting in version 4.2, MongoDB removes the Index Key Limit.
     * </ul>
     *
     * <p>As of now it is extracted by {@link MongoKeyExtractor} according to the primary key
     * specified by the Flink table schema.
     *
     * <ul>
     *   <li>When there's only a single field in the specified primary key, we convert the field
     *       data to bson value as _id of the corresponding document.
     *   <li>When there's multiple fields in the specified primary key, we convert and composite
     *       these fields into a {@link BsonType#DOCUMENT} as the _id of the corresponding document.
     *       For example, if have a primary key statement <code>PRIMARY KEY (f1, f2) NOT ENFORCED
     *       </code>, the extracted _id will be the form like <code>_id: {f1: v1, f2: v2}</code>
     * </ul>
     *
     * <p>The illegal types are mostly {@link LogicalTypeFamily#COLLECTION} types and {@link
     * LogicalTypeRoot#RAW} type and other types that cannot be converted to {@link BsonType} by
     * {@link RowDataToBsonConverters}.
     */
    public static void validatePrimaryKey(DataType primaryKeyDataType) {
        List<DataType> fieldDataTypes = DataType.getFieldDataTypes(primaryKeyDataType);
        List<LogicalTypeRoot> illegalTypes =
                fieldDataTypes.stream()
                        .map(DataType::getLogicalType)
                        .map(LogicalType::getTypeRoot)
                        .filter(t -> !ALLOWED_PRIMARY_KEY_TYPES.contains(t))
                        .collect(Collectors.toList());
        if (!illegalTypes.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "The table has a primary key on columns of illegal types: %s.",
                            illegalTypes));
        }
    }

    private MongoValidationUtils() {}
}
