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

package org.apache.flink.connector.mongodb.table.converter;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.json.JsonParseException;
import org.bson.types.Decimal128;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ENCODE_VALUE_FIELD;

/** Tool class used to convert from {@link RowData} to {@link BsonValue}. */
@Internal
public class RowDataToBsonConverters {

    // --------------------------------------------------------------------------------
    // Runtime Converters
    // --------------------------------------------------------------------------------

    /**
     * Runtime converter that converts objects of Flink Table & SQL internal data structures to
     * corresponding {@link BsonValue} data structures.
     */
    @FunctionalInterface
    public interface RowDataToBsonConverter extends Serializable {
        BsonValue convert(Object value);
    }

    // --------------------------------------------------------------------------------
    // IMPORTANT! We use anonymous classes instead of lambdas for a reason here. It is
    // necessary because the maven shade plugin cannot relocate classes in
    // SerializedLambdas (MSHADE-260). On the other hand we want to relocate Bson for
    // sql-connector uber jars.
    // --------------------------------------------------------------------------------

    public static RowDataToBsonConverter createConverter(LogicalType type) {
        return wrapIntoNullSafeInternalConverter(createInternalConverter(type), type);
    }

    private static RowDataToBsonConverter wrapIntoNullSafeInternalConverter(
            RowDataToBsonConverter rowDataToBsonConverter, LogicalType type) {
        return new RowDataToBsonConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public BsonValue convert(Object value) {
                if (value == null || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                    if (type.isNullable()) {
                        return BsonNull.VALUE;
                    } else {
                        throw new IllegalArgumentException(
                                "The column type is <"
                                        + type
                                        + ">, but a null value is being written into it");
                    }
                } else {
                    return rowDataToBsonConverter.convert(value);
                }
            }
        };
    }

    private static RowDataToBsonConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return new RowDataToBsonConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public BsonValue convert(Object value) {
                        return null;
                    }
                };
            case BOOLEAN:
                return new RowDataToBsonConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public BsonValue convert(Object value) {
                        return new BsonBoolean((boolean) value);
                    }
                };
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return new RowDataToBsonConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public BsonValue convert(Object value) {
                        return new BsonInt32((int) value);
                    }
                };
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return new RowDataToBsonConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public BsonValue convert(Object value) {
                        return new BsonInt64((long) value);
                    }
                };
            case DOUBLE:
                return new RowDataToBsonConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public BsonValue convert(Object value) {
                        return new BsonDouble((double) value);
                    }
                };
            case DECIMAL:
                return new RowDataToBsonConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public BsonValue convert(Object value) {
                        BigDecimal decimalVal = ((DecimalData) value).toBigDecimal();
                        return new BsonDecimal128(new Decimal128(decimalVal));
                    }
                };
            case CHAR:
            case VARCHAR:
                return new RowDataToBsonConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public BsonValue convert(Object value) {
                        String val = value.toString();
                        // try to parse out the mongodb specific data type from extend-json.
                        if (val.startsWith("{")
                                && val.endsWith("}")
                                && val.contains(ENCODE_VALUE_FIELD)) {
                            try {
                                BsonDocument doc = BsonDocument.parse(val);
                                if (doc.containsKey(ENCODE_VALUE_FIELD)) {
                                    return doc.get(ENCODE_VALUE_FIELD);
                                }
                            } catch (JsonParseException e) {
                                // invalid json format, fallback to store as a bson string.
                                return new BsonString(value.toString());
                            }
                        }
                        return new BsonString(value.toString());
                    }
                };
            case BINARY:
            case VARBINARY:
                return new RowDataToBsonConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public BsonValue convert(Object value) {
                        return new BsonBinary((byte[]) value);
                    }
                };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return new RowDataToBsonConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public BsonValue convert(Object value) {
                        return new BsonDateTime(((TimestampData) value).toTimestamp().getTime());
                    }
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new RowDataToBsonConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public BsonValue convert(Object value) {
                        return new BsonDateTime(((TimestampData) value).getMillisecond());
                    }
                };
            case ROW:
                return createRowConverter((RowType) type);
            case ARRAY:
                return createArrayConverter((ArrayType) type);
            case MAP:
                return createMapConverter((MapType) type);
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    private static RowDataToBsonConverter createRowConverter(RowType rowType) {
        final RowDataToBsonConverter[] fieldConverters =
                rowType.getChildren().stream()
                        .map(RowDataToBsonConverters::createConverter)
                        .toArray(RowDataToBsonConverter[]::new);
        final LogicalType[] fieldTypes =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);

        final int fieldCount = rowType.getFieldCount();
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
        }

        return new RowDataToBsonConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public BsonValue convert(Object value) {
                final RowData rowData = (RowData) value;
                final BsonDocument document = new BsonDocument();
                for (int i = 0; i < fieldCount; i++) {
                    String fieldName = rowType.getFieldNames().get(i);
                    Object fieldValue = fieldGetters[i].getFieldOrNull(rowData);
                    document.append(fieldName, fieldConverters[i].convert(fieldValue));
                }
                return document;
            }
        };
    }

    private static RowDataToBsonConverter createArrayConverter(ArrayType arrayType) {
        final LogicalType elementType = arrayType.getElementType();
        final ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
        final RowDataToBsonConverter elementConverter = createConverter(elementType);

        return new RowDataToBsonConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public BsonValue convert(Object value) {
                final ArrayData arrayData = (ArrayData) value;
                final List<BsonValue> bsonValues = new ArrayList<>();
                for (int i = 0; i < arrayData.size(); i++) {
                    final BsonValue bsonValue =
                            elementConverter.convert(elementGetter.getElementOrNull(arrayData, i));
                    bsonValues.add(bsonValue);
                }
                return new BsonArray(bsonValues);
            }
        };
    }

    private static RowDataToBsonConverter createMapConverter(MapType mapType) {
        final LogicalType keyType = mapType.getKeyType();
        final LogicalType valueType = mapType.getValueType();
        if (!keyType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException(
                    "MongoDB doesn't support non-string as key type of map. "
                            + "The type is: "
                            + keyType.asSummaryString());
        }
        final RowDataToBsonConverter valueConverter = createConverter(valueType);
        final ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);

        return new RowDataToBsonConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public BsonValue convert(Object value) {
                final MapData mapData = (MapData) value;
                final ArrayData keyArray = mapData.keyArray();
                final ArrayData valueArray = mapData.valueArray();
                final BsonDocument document = new BsonDocument();
                for (int i = 0; i < mapData.size(); i++) {
                    final String key = keyArray.getString(i).toString();
                    final BsonValue bsonValue =
                            valueConverter.convert(valueGetter.getElementOrNull(valueArray, i));
                    document.append(key, bsonValue);
                }
                return document;
            }
        };
    }
}
