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
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.function.SerializableFunction;

import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.types.Decimal128;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.DEFAULT_JSON_WRITER_SETTINGS;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ENCODE_VALUE_FIELD;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Tool class used to convert from {@link BsonValue} to {@link RowData}. * */
@Internal
public class BsonToRowDataConverters {

    // -------------------------------------------------------------------------------------
    // Runtime Converters
    // -------------------------------------------------------------------------------------

    /**
     * Runtime converter that converts {@link BsonValue} into objects of Flink Table & SQL internal
     * data structures.
     */
    @FunctionalInterface
    public interface BsonToRowDataConverter extends Serializable {
        RowData convert(BsonDocument bsonDocument);
    }

    // --------------------------------------------------------------------------------
    // IMPORTANT! We use anonymous classes instead of lambdas for a reason here. It is
    // necessary because the maven shade plugin cannot relocate classes in
    // SerializedLambdas (MSHADE-260). On the other hand we want to relocate Bson for
    // sql-connector uber jars.
    // --------------------------------------------------------------------------------

    public static BsonToRowDataConverter createConverter(RowType rowType) {
        SerializableFunction<BsonValue, Object> internalRowConverter =
                createNullSafeInternalConverter(rowType);
        return new BsonToRowDataConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public RowData convert(BsonDocument bsonDocument) {
                return (RowData) internalRowConverter.apply(bsonDocument);
            }
        };
    }

    private static SerializableFunction<BsonValue, Object> createNullSafeInternalConverter(
            LogicalType type) {
        return wrapIntoNullSafeInternalConverter(createInternalConverter(type), type);
    }

    private static SerializableFunction<BsonValue, Object> wrapIntoNullSafeInternalConverter(
            SerializableFunction<BsonValue, Object> internalConverter, LogicalType type) {
        return new SerializableFunction<BsonValue, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object apply(BsonValue bsonValue) {
                if (isBsonValueNull(bsonValue) || isBsonDecimalNaN(bsonValue)) {
                    if (type.isNullable()) {
                        return null;
                    } else {
                        throw new IllegalArgumentException(
                                "Unable to convert to <"
                                        + type
                                        + "> from nullable value "
                                        + bsonValue);
                    }
                }
                return internalConverter.apply(bsonValue);
            }
        };
    }

    private static boolean isBsonValueNull(BsonValue bsonValue) {
        return bsonValue == null
                || bsonValue.isNull()
                || bsonValue.getBsonType() == BsonType.UNDEFINED;
    }

    private static boolean isBsonDecimalNaN(BsonValue bsonValue) {
        return bsonValue.isDecimal128() && bsonValue.asDecimal128().getValue().isNaN();
    }

    /** Creates a runtime converter which assuming input object is not null. */
    private static SerializableFunction<BsonValue, Object> createInternalConverter(
            LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return new SerializableFunction<BsonValue, Object>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object apply(BsonValue bsonValue) {
                        return null;
                    }
                };
            case BOOLEAN:
                return new SerializableFunction<BsonValue, Object>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object apply(BsonValue bsonValue) {
                        return convertToBoolean(bsonValue);
                    }
                };
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return new SerializableFunction<BsonValue, Object>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object apply(BsonValue bsonValue) {
                        return convertToInt(bsonValue);
                    }
                };
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return new SerializableFunction<BsonValue, Object>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object apply(BsonValue bsonValue) {
                        return convertToLong(bsonValue);
                    }
                };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return new SerializableFunction<BsonValue, Object>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object apply(BsonValue bsonValue) {
                        return TimestampData.fromLocalDateTime(convertToLocalDateTime(bsonValue));
                    }
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new SerializableFunction<BsonValue, Object>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object apply(BsonValue bsonValue) {
                        return TimestampData.fromInstant(convertToInstant(bsonValue));
                    }
                };
            case DOUBLE:
                return new SerializableFunction<BsonValue, Object>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object apply(BsonValue bsonValue) {
                        return convertToDouble(bsonValue);
                    }
                };
            case CHAR:
            case VARCHAR:
                return new SerializableFunction<BsonValue, Object>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object apply(BsonValue bsonValue) {
                        return StringData.fromString(convertToString(bsonValue));
                    }
                };
            case BINARY:
            case VARBINARY:
                return new SerializableFunction<BsonValue, Object>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object apply(BsonValue bsonValue) {
                        return convertToBinary(bsonValue);
                    }
                };
            case DECIMAL:
                return new SerializableFunction<BsonValue, Object>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object apply(BsonValue bsonValue) {
                        DecimalType decimalType = (DecimalType) type;
                        BigDecimal decimalValue = convertToBigDecimal(bsonValue);
                        return DecimalData.fromBigDecimal(
                                decimalValue, decimalType.getPrecision(), decimalType.getScale());
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
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static SerializableFunction<BsonValue, Object> createRowConverter(RowType rowType) {
        final SerializableFunction<BsonValue, Object>[] fieldConverters =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(BsonToRowDataConverters::createNullSafeInternalConverter)
                        .toArray(SerializableFunction[]::new);
        final int arity = rowType.getFieldCount();
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

        return new SerializableFunction<BsonValue, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object apply(BsonValue bsonValue) {
                if (!bsonValue.isDocument()) {
                    throw new IllegalArgumentException(
                            "Unable to convert to rowType from unexpected value '"
                                    + bsonValue
                                    + "' of type "
                                    + bsonValue.getBsonType());
                }

                BsonDocument document = bsonValue.asDocument();
                GenericRowData row = new GenericRowData(arity);
                for (int i = 0; i < arity; i++) {
                    String fieldName = fieldNames[i];
                    BsonValue fieldValue = document.get(fieldName);
                    Object convertedField = fieldConverters[i].apply(fieldValue);
                    row.setField(i, convertedField);
                }
                return row;
            }
        };
    }

    private static SerializableFunction<BsonValue, Object> createArrayConverter(
            ArrayType arrayType) {
        final SerializableFunction<BsonValue, Object> elementConverter =
                createNullSafeInternalConverter(arrayType.getElementType());
        return new SerializableFunction<BsonValue, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object apply(BsonValue bsonValue) {
                if (!bsonValue.isArray()) {
                    throw new IllegalArgumentException(
                            "Unable to convert to arrayType from unexpected value '"
                                    + bsonValue
                                    + "' of type "
                                    + bsonValue.getBsonType());
                }

                List<BsonValue> in = bsonValue.asArray();
                final Object[] elementArray = new Object[in.size()];
                for (int i = 0; i < in.size(); i++) {
                    elementArray[i] = elementConverter.apply(in.get(i));
                }
                return new GenericArrayData(elementArray);
            }
        };
    }

    private static SerializableFunction<BsonValue, Object> createMapConverter(MapType mapType) {
        LogicalType keyType = mapType.getKeyType();
        checkArgument(keyType.supportsInputConversion(String.class));

        LogicalType valueType = mapType.getValueType();
        SerializableFunction<BsonValue, Object> valueConverter =
                createNullSafeInternalConverter(valueType);

        return new SerializableFunction<BsonValue, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object apply(BsonValue bsonValue) {
                if (!bsonValue.isDocument()) {
                    throw new IllegalArgumentException(
                            "Unable to convert to rowType from unexpected value '"
                                    + bsonValue
                                    + "' of type "
                                    + bsonValue.getBsonType());
                }

                BsonDocument document = bsonValue.asDocument();
                Map<StringData, Object> map = new HashMap<>();
                for (String key : document.keySet()) {
                    map.put(StringData.fromString(key), valueConverter.apply(document.get(key)));
                }
                return new GenericMapData(map);
            }
        };
    }

    private static boolean convertToBoolean(BsonValue bsonValue) {
        if (bsonValue.isBoolean()) {
            return bsonValue.asBoolean().getValue();
        }
        throw new IllegalArgumentException(
                "Unable to convert to boolean from unexpected value '"
                        + bsonValue
                        + "' of type "
                        + bsonValue.getBsonType());
    }

    private static int convertToInt(BsonValue bsonValue) {
        if (bsonValue.isInt32()) {
            return bsonValue.asNumber().intValue();
        }
        throw new IllegalArgumentException(
                "Unable to convert to integer from unexpected value '"
                        + bsonValue
                        + "' of type "
                        + bsonValue.getBsonType());
    }

    private static long convertToLong(BsonValue bsonValue) {
        if (bsonValue.isInt64()) {
            return bsonValue.asNumber().longValue();
        }
        throw new IllegalArgumentException(
                "Unable to convert to long from unexpected value '"
                        + bsonValue
                        + "' of type "
                        + bsonValue.getBsonType());
    }

    private static double convertToDouble(BsonValue bsonValue) {
        if (bsonValue.isDouble()) {
            return bsonValue.asNumber().doubleValue();
        }
        throw new IllegalArgumentException(
                "Unable to convert to double from unexpected value '"
                        + bsonValue
                        + "' of type "
                        + bsonValue.getBsonType());
    }

    private static Instant convertToInstant(BsonValue bsonValue) {
        if (bsonValue.isTimestamp()) {
            return Instant.ofEpochSecond(bsonValue.asTimestamp().getTime());
        }
        if (bsonValue.isDateTime()) {
            return Instant.ofEpochMilli(bsonValue.asDateTime().getValue());
        }
        throw new IllegalArgumentException(
                "Unable to convert to Instant from unexpected value '"
                        + bsonValue
                        + "' of type "
                        + bsonValue.getBsonType());
    }

    private static LocalDateTime convertToLocalDateTime(BsonValue bsonValue) {
        Instant instant;
        if (bsonValue.isTimestamp()) {
            instant = Instant.ofEpochSecond(bsonValue.asTimestamp().getTime());
        } else if (bsonValue.isDateTime()) {
            instant = Instant.ofEpochMilli(bsonValue.asDateTime().getValue());
        } else {
            throw new IllegalArgumentException(
                    "Unable to convert to LocalDateTime from unexpected value '"
                            + bsonValue
                            + "' of type "
                            + bsonValue.getBsonType());
        }
        return Timestamp.from(instant).toLocalDateTime();
    }

    private static byte[] convertToBinary(BsonValue bsonValue) {
        if (bsonValue.isBinary()) {
            return bsonValue.asBinary().getData();
        }
        throw new IllegalArgumentException(
                "Unsupported BYTES value type: " + bsonValue.getClass().getSimpleName());
    }

    private static String convertToString(BsonValue bsonValue) {
        if (bsonValue.isString()) {
            return bsonValue.asString().getValue();
        }
        if (bsonValue.isObjectId()) {
            return bsonValue.asObjectId().getValue().toHexString();
        }
        return new BsonDocument(ENCODE_VALUE_FIELD, bsonValue).toJson(DEFAULT_JSON_WRITER_SETTINGS);
    }

    private static BigDecimal convertToBigDecimal(BsonValue bsonValue) {
        if (bsonValue.isDecimal128()) {
            Decimal128 decimal128Value = bsonValue.asDecimal128().decimal128Value();
            if (decimal128Value.isFinite()) {
                return bsonValue.asDecimal128().decimal128Value().bigDecimalValue();
            } else {
                // DecimalData doesn't have the concept of infinity.
                throw new IllegalArgumentException(
                        "Unable to convert infinite bson decimal to Decimal type.");
            }
        }
        throw new IllegalArgumentException(
                "Unable to convert to decimal from unexpected value '"
                        + bsonValue
                        + "' of type "
                        + bsonValue.getBsonType());
    }
}
