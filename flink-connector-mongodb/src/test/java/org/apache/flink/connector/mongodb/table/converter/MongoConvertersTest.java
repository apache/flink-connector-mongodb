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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDbPointer;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link BsonToRowDataConverters} and {@link RowDataToBsonConverters}. */
public class MongoConvertersTest {

    @Test
    public void testConvertBsonToRowData() {
        DataType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("_id", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("f0", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("f1", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("f2", DataTypes.INT().notNull()),
                        DataTypes.FIELD("f3", DataTypes.BIGINT().notNull()),
                        DataTypes.FIELD("f4", DataTypes.DOUBLE().notNull()),
                        DataTypes.FIELD("f5", DataTypes.DECIMAL(10, 2).notNull()),
                        DataTypes.FIELD("f6", DataTypes.BOOLEAN().notNull()),
                        DataTypes.FIELD("f7", DataTypes.TIMESTAMP_LTZ(0).notNull()),
                        DataTypes.FIELD("f8", DataTypes.TIMESTAMP_LTZ(3).notNull()),
                        DataTypes.FIELD("f9", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("f10", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("f11", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("f12", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("f13", DataTypes.STRING().notNull()),
                        DataTypes.FIELD(
                                "f14", DataTypes.ROW(DataTypes.FIELD("f14_k", DataTypes.INT()))),
                        DataTypes.FIELD("f15", DataTypes.STRING().notNull()),
                        DataTypes.FIELD(
                                "f16",
                                DataTypes.ARRAY(
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "f16_k", DataTypes.DOUBLE())))
                                        .notNull()),
                        DataTypes.FIELD("f17", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("f18", DataTypes.NULL()),
                        DataTypes.FIELD("f19", DataTypes.NULL()),
                        DataTypes.FIELD("f20", DataTypes.DECIMAL(10, 2).nullable()));

        ObjectId oid = new ObjectId();
        UUID uuid = UUID.fromString("811faa5d-a984-46bf-8bd0-8cb1bcef5d69");
        Instant now = Instant.now();

        BsonDocument docWithFullTypes =
                new BsonDocument()
                        .append("_id", new BsonObjectId(oid))
                        .append("f0", new BsonString("string"))
                        .append("f1", new BsonBinary(uuid))
                        .append("f2", new BsonInt32(2))
                        .append("f3", new BsonInt64(3L))
                        .append("f4", new BsonDouble(4.1d))
                        .append("f5", new BsonDecimal128(new Decimal128(new BigDecimal("5.1"))))
                        .append("f6", BsonBoolean.FALSE)
                        .append("f7", new BsonTimestamp((int) now.getEpochSecond(), 0))
                        .append("f8", new BsonDateTime(now.toEpochMilli()))
                        .append(
                                "f9",
                                new BsonRegularExpression(Pattern.compile("^9$").pattern(), "i"))
                        .append("f10", new BsonJavaScript("function() { return 10; }"))
                        .append(
                                "f11",
                                new BsonJavaScriptWithScope(
                                        "function() { return 11; }", new BsonDocument()))
                        .append("f12", new BsonSymbol("12"))
                        .append(
                                "f13",
                                new BsonDbPointer(
                                        "db.coll", new ObjectId("63932a00da01604af329e33c")))
                        .append("f14", new BsonDocument("f14_k", new BsonInt32(14)))
                        .append("f15", new BsonDocument("f15_k", new BsonInt32(15)))
                        .append(
                                "f16",
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonDocument("f16_k", new BsonDouble(16.1d)),
                                                new BsonDocument("f16_k", new BsonDouble(16.2d)))))
                        .append(
                                "f17",
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonDocument("f17_k", new BsonDouble(17.1d)),
                                                new BsonDocument("f17_k", new BsonDouble(17.2d)))))
                        .append("f18", new BsonNull())
                        .append("f19", new BsonUndefined())
                        .append("f20", new BsonDecimal128(Decimal128.NaN));

        RowData expect =
                GenericRowData.of(
                        StringData.fromString(oid.toHexString()),
                        StringData.fromString("string"),
                        StringData.fromString(
                                "{\"_value\": {\"$binary\": {\"base64\": \"gR+qXamERr+L0IyxvO9daQ==\", \"subType\": \"04\"}}}"),
                        2,
                        3L,
                        4.1d,
                        DecimalData.fromBigDecimal(new BigDecimal("5.1"), 10, 2),
                        false,
                        TimestampData.fromEpochMillis(now.getEpochSecond() * 1000),
                        TimestampData.fromEpochMillis(now.toEpochMilli()),
                        StringData.fromString(
                                "{\"_value\": {\"$regularExpression\": {\"pattern\": \"^9$\", \"options\": \"i\"}}}"),
                        StringData.fromString(
                                "{\"_value\": {\"$code\": \"function() { return 10; }\"}}"),
                        StringData.fromString(
                                "{\"_value\": {\"$code\": \"function() { return 11; }\", \"$scope\": {}}}"),
                        StringData.fromString("{\"_value\": {\"$symbol\": \"12\"}}"),
                        StringData.fromString(
                                "{\"_value\": {\"$dbPointer\": {\"$ref\": \"db.coll\", \"$id\": {\"$oid\": \"63932a00da01604af329e33c\"}}}}"),
                        GenericRowData.of(14),
                        StringData.fromString(
                                "{\"_value\": {\"f15_k\": {\"$numberInt\": \"15\"}}}"),
                        new GenericArrayData(
                                new RowData[] {GenericRowData.of(16.1d), GenericRowData.of(16.2d)}),
                        StringData.fromString(
                                "{\"_value\": [{\"f17_k\": {\"$numberDouble\": \"17.1\"}}, {\"f17_k\": {\"$numberDouble\": \"17.2\"}}]}"),
                        null,
                        null,
                        null);

        // Test convert Bson to RowData
        BsonToRowDataConverters.BsonToRowDataConverter bsonToRowDataConverter =
                BsonToRowDataConverters.createConverter((RowType) rowType.getLogicalType());
        RowData actual = (RowData) bsonToRowDataConverter.convert(docWithFullTypes);
        assertThat(actual).isEqualTo(expect);
    }

    @Test
    public void testConvertBsonNullToRowDataWithNonNullableConstraints() {
        DataType rowType = DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.STRING().notNull()));

        BsonDocument docWithNullValue = new BsonDocument("f0", new BsonNull());

        BsonToRowDataConverters.BsonToRowDataConverter bsonToRowDataConverter =
                BsonToRowDataConverters.createConverter((RowType) rowType.getLogicalType());

        assertThatThrownBy(() -> bsonToRowDataConverter.convert(docWithNullValue))
                .hasStackTraceContaining(
                        "Unable to convert to <STRING NOT NULL> from nullable value BsonNull");
    }

    @Test
    public void testConvertBsonNumberAndBooleanToSqlString() {
        DataType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("f0", DataTypes.STRING()),
                        DataTypes.FIELD("f1", DataTypes.STRING()),
                        DataTypes.FIELD("f2", DataTypes.STRING()),
                        DataTypes.FIELD("f3", DataTypes.STRING()),
                        DataTypes.FIELD("f4", DataTypes.STRING()));

        BsonDocument document =
                new BsonDocument()
                        .append("f0", BsonBoolean.FALSE)
                        .append("f1", new BsonInt32(-1))
                        .append("f2", new BsonInt64(127L))
                        .append("f3", new BsonDouble(127.11d))
                        .append("f4", new BsonDecimal128(new Decimal128(new BigDecimal("127.11"))));

        RowData expect =
                GenericRowData.of(
                        StringData.fromString("{\"_value\": false}"),
                        StringData.fromString("{\"_value\": {\"$numberInt\": \"-1\"}}"),
                        StringData.fromString("{\"_value\": {\"$numberLong\": \"127\"}}"),
                        StringData.fromString("{\"_value\": {\"$numberDouble\": \"127.11\"}}"),
                        StringData.fromString("{\"_value\": {\"$numberDecimal\": \"127.11\"}}"));

        // Test for compatible bson number and boolean to string sql type conversions
        BsonToRowDataConverters.BsonToRowDataConverter bsonToRowDataConverter =
                BsonToRowDataConverters.createConverter((RowType) rowType.getLogicalType());
        RowData actual = (RowData) bsonToRowDataConverter.convert(document);
        assertThat(actual).isEqualTo(expect);
    }

    @Test
    public void testConvertBsonToSqlBoolean() {
        DataType rowType = DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.BOOLEAN()));

        BsonDocument document = new BsonDocument("f0", BsonBoolean.FALSE);

        RowData expect = GenericRowData.of(false);

        // Test for compatible boolean sql type conversions
        BsonToRowDataConverters.BsonToRowDataConverter bsonToRowDataConverter =
                BsonToRowDataConverters.createConverter((RowType) rowType.getLogicalType());
        RowData actual = (RowData) bsonToRowDataConverter.convert(document);
        assertThat(actual).isEqualTo(expect);
    }

    @Test
    public void testConvertBsonToSqlTinyInt() {
        DataType rowType = DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.TINYINT()));
        assertThatThrownBy(
                        () ->
                                BsonToRowDataConverters.createConverter(
                                        (RowType) rowType.getLogicalType()))
                .hasStackTraceContaining("Unsupported type: TINYINT");
    }

    @Test
    public void testConvertBsonToSqlSmallInt() {
        DataType rowType = DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.SMALLINT()));
        assertThatThrownBy(
                        () ->
                                BsonToRowDataConverters.createConverter(
                                        (RowType) rowType.getLogicalType()))
                .hasStackTraceContaining("Unsupported type: SMALLINT");
    }

    @Test
    public void testConvertBsonToSqlInt() {
        DataType rowType = DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.INT()));

        BsonDocument document = new BsonDocument("f0", new BsonInt32(-1));

        RowData expect = GenericRowData.of(-1);

        // Test for compatible int sql type conversions
        BsonToRowDataConverters.BsonToRowDataConverter bsonToRowDataConverter =
                BsonToRowDataConverters.createConverter((RowType) rowType.getLogicalType());
        RowData actual = (RowData) bsonToRowDataConverter.convert(document);
        assertThat(actual).isEqualTo(expect);
    }

    @Test
    public void testConvertBsonToSqlBigInt() {
        DataType rowType = DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.BIGINT()));

        BsonDocument document = new BsonDocument("f0", new BsonInt64(127L));

        RowData expect = GenericRowData.of(127L);

        // Test for compatible int sql type conversions
        BsonToRowDataConverters.BsonToRowDataConverter bsonToRowDataConverter =
                BsonToRowDataConverters.createConverter((RowType) rowType.getLogicalType());
        RowData actual = (RowData) bsonToRowDataConverter.convert(document);
        assertThat(actual).isEqualTo(expect);
    }

    @Test
    public void testConvertBsonToSqlDouble() {
        DataType rowType = DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.DOUBLE()));

        BsonDocument document = new BsonDocument("f0", new BsonDouble(127.11d));

        RowData expect = GenericRowData.of(127.11d);

        // Test for compatible double sql type conversions
        BsonToRowDataConverters.BsonToRowDataConverter bsonToRowDataConverter =
                BsonToRowDataConverters.createConverter((RowType) rowType.getLogicalType());
        RowData actual = (RowData) bsonToRowDataConverter.convert(document);
        assertThat(actual).isEqualTo(expect);
    }

    @Test
    public void testConvertBsonToSqlFloat() {
        DataType rowType = DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.FLOAT()));
        assertThatThrownBy(
                        () ->
                                BsonToRowDataConverters.createConverter(
                                        (RowType) rowType.getLogicalType()))
                .hasStackTraceContaining("Unsupported type: FLOAT");
    }

    @Test
    public void testConvertBsonToSqlDecimal() {
        DataType rowType = DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.DECIMAL(10, 2)));

        BsonDocument document =
                new BsonDocument()
                        .append("f0", new BsonDecimal128(new Decimal128(new BigDecimal("127.11"))));

        RowData expect =
                GenericRowData.of(DecimalData.fromBigDecimal(new BigDecimal("127.11"), 10, 2));

        // Test for compatible float sql type conversions
        BsonToRowDataConverters.BsonToRowDataConverter bsonToRowDataConverter =
                BsonToRowDataConverters.createConverter((RowType) rowType.getLogicalType());
        RowData actual = (RowData) bsonToRowDataConverter.convert(document);
        assertThat(actual).isEqualTo(expect);
    }

    @Test
    public void testConvertBsonInfiniteDecimalToSqlNumber() {
        BsonDecimal128 positiveInfinity = new BsonDecimal128(Decimal128.POSITIVE_INFINITY);
        BsonDecimal128 negativeInfinity = new BsonDecimal128(Decimal128.NEGATIVE_INFINITY);

        DataType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("f0", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("f1", DataTypes.DECIMAL(10, 2)));

        BsonDocument document =
                new BsonDocument().append("f0", positiveInfinity).append("f1", negativeInfinity);

        // Test for compatible decimal sql type conversions
        BsonToRowDataConverters.BsonToRowDataConverter bsonToRowDataConverter =
                BsonToRowDataConverters.createConverter((RowType) rowType.getLogicalType());

        assertThatThrownBy(() -> bsonToRowDataConverter.convert(document))
                .hasStackTraceContaining(
                        "Unable to convert infinite bson decimal to Decimal type.");
    }

    @Test
    public void testConvertRowDataToBson() {
        DataType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("_id", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("f0", DataTypes.BOOLEAN().notNull()),
                        DataTypes.FIELD("f1", DataTypes.INT().notNull()),
                        DataTypes.FIELD("f2", DataTypes.BIGINT().notNull()),
                        DataTypes.FIELD("f3", DataTypes.DOUBLE().notNull()),
                        DataTypes.FIELD("f4", DataTypes.DECIMAL(10, 2).notNull()),
                        DataTypes.FIELD("f5", DataTypes.TIMESTAMP_LTZ(6).notNull()),
                        DataTypes.FIELD(
                                "f6",
                                DataTypes.ROW(DataTypes.FIELD("f6_k", DataTypes.BIGINT()))
                                        .notNull()),
                        DataTypes.FIELD(
                                "f7",
                                DataTypes.ARRAY(
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "f7_k", DataTypes.DOUBLE())))
                                        .notNull()));

        ObjectId oid = new ObjectId();
        Instant now = Instant.now();

        RowData rowData =
                GenericRowData.of(
                        StringData.fromString(oid.toHexString()),
                        false,
                        1,
                        4L,
                        6.1d,
                        DecimalData.fromBigDecimal(new BigDecimal("7.1"), 10, 2),
                        TimestampData.fromEpochMillis(now.toEpochMilli()),
                        GenericRowData.of(9L),
                        new GenericArrayData(
                                new RowData[] {
                                    GenericRowData.of(10.1d), GenericRowData.of(10.2d)
                                }));

        BsonDocument expect =
                new BsonDocument()
                        .append("_id", new BsonString(oid.toHexString()))
                        .append("f0", BsonBoolean.FALSE)
                        .append("f1", new BsonInt32(1))
                        .append("f2", new BsonInt64(4))
                        .append("f3", new BsonDouble(6.1d))
                        .append("f4", new BsonDecimal128(new Decimal128(new BigDecimal("7.10"))))
                        .append("f5", new BsonDateTime(now.toEpochMilli()))
                        .append("f6", new BsonDocument("f6_k", new BsonInt64(9L)))
                        .append(
                                "f7",
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonDocument("f7_k", new BsonDouble(10.1d)),
                                                new BsonDocument("f7_k", new BsonDouble(10.2d)))));

        // Test convert RowData to Bson
        RowDataToBsonConverters.RowDataToBsonConverter rowDataToBsonConverter =
                RowDataToBsonConverters.createConverter(rowType.getLogicalType());
        BsonDocument actual = (BsonDocument) rowDataToBsonConverter.convert(rowData);
        assertThat(actual).isEqualTo(expect);
    }

    @Test
    public void testConvertRowDataNullValueToBsonWithNonNullableConstraints() {
        DataType rowType = DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.STRING().notNull()));

        RowData rowData = GenericRowData.of((StringData) null);

        RowDataToBsonConverters.RowDataToBsonConverter rowDataToBsonConverter =
                RowDataToBsonConverters.createConverter(rowType.getLogicalType());

        assertThatThrownBy(() -> rowDataToBsonConverter.convert(rowData))
                .hasStackTraceContaining(
                        "The column type is <STRING NOT NULL>, but a null value is being written into it");
    }
}
