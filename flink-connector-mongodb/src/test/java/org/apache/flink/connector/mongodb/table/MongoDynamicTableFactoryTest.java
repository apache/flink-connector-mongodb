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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.mongodb.common.config.MongoConnectionOptions;
import org.apache.flink.connector.mongodb.sink.config.MongoWriteOptions;
import org.apache.flink.connector.mongodb.source.config.MongoReadOptions;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.COLLECTION;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.DATABASE;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.DELIVERY_GUARANTEE;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.LOOKUP_RETRY_INTERVAL;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.SCAN_CURSOR_BATCH_SIZE;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.SCAN_CURSOR_NO_TIMEOUT;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.SCAN_FETCH_SIZE;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.SCAN_PARTITION_SAMPLES;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.SCAN_PARTITION_SIZE;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.SCAN_PARTITION_STRATEGY;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.SINK_MAX_RETRIES;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.SINK_RETRY_INTERVAL;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.URI;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for {@link MongoDynamicTableSource} and {@link MongoDynamicTableSink} created by {@link
 * MongoDynamicTableFactory}.
 */
public class MongoDynamicTableFactoryTest {

    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("aaa", DataTypes.INT().notNull()),
                            Column.physical("bbb", DataTypes.STRING().notNull()),
                            Column.physical("ccc", DataTypes.DOUBLE()),
                            Column.physical("ddd", DataTypes.DECIMAL(31, 18)),
                            Column.physical("eee", DataTypes.TIMESTAMP(3))),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("name", Arrays.asList("bbb", "aaa")));

    @Test
    public void testMongoSourceCommonProperties() {
        DynamicTableSource actualSource = createTableSource(SCHEMA, getRequiredOptions());

        MongoDynamicTableSource expectedSource =
                new MongoDynamicTableSource(
                        getConnectionOptions(),
                        MongoReadOptions.builder().build(),
                        null,
                        LookupOptions.MAX_RETRIES.defaultValue(),
                        LOOKUP_RETRY_INTERVAL.defaultValue().toMillis(),
                        SCHEMA.toPhysicalRowDataType());
        assertThat(actualSource).isEqualTo(expectedSource);
    }

    @Test
    public void testMongoSinkCommonProperties() {
        DynamicTableSink actualSink = createTableSink(SCHEMA, getRequiredOptions());

        MongoDynamicTableSink expectedSink =
                new MongoDynamicTableSink(
                        getConnectionOptions(),
                        MongoWriteOptions.builder().build(),
                        null,
                        SCHEMA.getPrimaryKey().isPresent(),
                        SCHEMA.toPhysicalRowDataType(),
                        MongoKeyExtractor.createKeyExtractor(SCHEMA));
        assertThat(actualSink).isEqualTo(expectedSink);
    }

    @Test
    public void testMongoReadProperties() {
        Map<String, String> properties = getRequiredOptions();
        properties.put(SCAN_FETCH_SIZE.key(), "1024");
        properties.put(SCAN_CURSOR_BATCH_SIZE.key(), "2048");
        properties.put(SCAN_CURSOR_NO_TIMEOUT.key(), "false");
        properties.put(SCAN_PARTITION_STRATEGY.key(), "split-vector");
        properties.put(SCAN_PARTITION_SIZE.key(), "128m");
        properties.put(SCAN_PARTITION_SAMPLES.key(), "5");

        DynamicTableSource actual = createTableSource(SCHEMA, properties);

        MongoConnectionOptions connectionOptions = getConnectionOptions();
        MongoReadOptions readOptions =
                MongoReadOptions.builder()
                        .setFetchSize(1024)
                        .setCursorBatchSize(2048)
                        .setNoCursorTimeout(false)
                        .setPartitionStrategy(PartitionStrategy.SPLIT_VECTOR)
                        .setPartitionSize(MemorySize.ofMebiBytes(128))
                        .setSamplesPerPartition(5)
                        .build();

        MongoDynamicTableSource expected =
                new MongoDynamicTableSource(
                        connectionOptions,
                        readOptions,
                        null,
                        LookupOptions.MAX_RETRIES.defaultValue(),
                        LOOKUP_RETRY_INTERVAL.defaultValue().toMillis(),
                        SCHEMA.toPhysicalRowDataType());

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testMongoLookupProperties() {
        Map<String, String> properties = getRequiredOptions();
        properties.put(LookupOptions.CACHE_TYPE.key(), "PARTIAL");
        properties.put(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE.key(), "10s");
        properties.put(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS.key(), "20s");
        properties.put(LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY.key(), "false");
        properties.put(LookupOptions.PARTIAL_CACHE_MAX_ROWS.key(), "15213");
        properties.put(LookupOptions.MAX_RETRIES.key(), "10");
        properties.put(LOOKUP_RETRY_INTERVAL.key(), "20ms");

        DynamicTableSource actual = createTableSource(SCHEMA, properties);

        MongoConnectionOptions connectionOptions = getConnectionOptions();

        MongoDynamicTableSource expected =
                new MongoDynamicTableSource(
                        connectionOptions,
                        MongoReadOptions.builder().build(),
                        DefaultLookupCache.fromConfig(Configuration.fromMap(properties)),
                        10,
                        20,
                        SCHEMA.toPhysicalRowDataType());

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testMongoSinkProperties() {
        Map<String, String> properties = getRequiredOptions();
        properties.put(BUFFER_FLUSH_MAX_ROWS.key(), "1001");
        properties.put(BUFFER_FLUSH_INTERVAL.key(), "2min");
        properties.put(DELIVERY_GUARANTEE.key(), "at-least-once");
        properties.put(SINK_MAX_RETRIES.key(), "5");
        properties.put(SINK_RETRY_INTERVAL.key(), "2s");

        DynamicTableSink actual = createTableSink(SCHEMA, properties);

        MongoConnectionOptions connectionOptions = getConnectionOptions();
        MongoWriteOptions writeOptions =
                MongoWriteOptions.builder()
                        .setBatchSize(1001)
                        .setBatchIntervalMs(TimeUnit.MINUTES.toMillis(2))
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setMaxRetries(5)
                        .setRetryIntervalMs(TimeUnit.SECONDS.toMillis(2))
                        .build();

        MongoDynamicTableSink expected =
                new MongoDynamicTableSink(
                        connectionOptions,
                        writeOptions,
                        null,
                        SCHEMA.getPrimaryKey().isPresent(),
                        SCHEMA.toPhysicalRowDataType(),
                        MongoKeyExtractor.createKeyExtractor(SCHEMA));

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testMongoSinkWithParallelism() {
        Map<String, String> properties = getRequiredOptions();
        properties.put("sink.parallelism", "2");

        DynamicTableSink actual = createTableSink(SCHEMA, properties);

        MongoConnectionOptions connectionOptions = getConnectionOptions();

        MongoWriteOptions writeOptions = MongoWriteOptions.builder().build();

        MongoDynamicTableSink expected =
                new MongoDynamicTableSink(
                        connectionOptions,
                        writeOptions,
                        2,
                        SCHEMA.getPrimaryKey().isPresent(),
                        SCHEMA.toPhysicalRowDataType(),
                        MongoKeyExtractor.createKeyExtractor(SCHEMA));

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testMongoValidation() {
        // fetch size lower than 1
        assertSourceValidationRejects(
                SCAN_FETCH_SIZE.key(), "0", "The fetch size must be larger than 0.");

        // cursor batch size lower than 0
        assertSourceValidationRejects(
                SCAN_CURSOR_BATCH_SIZE.key(),
                "-1",
                "The cursor batch size must be larger than or equal to 0");

        // partition memory size lower than 1mb
        assertSourceValidationRejects(
                SCAN_PARTITION_SIZE.key(),
                "900kb",
                "The partition size must be larger than or equal to 1mb.");

        // samples per partition lower than 1
        assertSourceValidationRejects(
                SCAN_PARTITION_SAMPLES.key(),
                "0",
                "The samples per partition must be larger than 0.");

        // lookup retry times shouldn't be negative
        assertSourceValidationRejects(
                LookupOptions.MAX_RETRIES.key(),
                "-1",
                "The 'lookup.max-retries' must be larger than or equal to 0.");

        // lookup retry interval shouldn't be 0
        assertSourceValidationRejects(
                LOOKUP_RETRY_INTERVAL.key(),
                "0ms",
                "The 'lookup.retry.interval' must be larger than 0.");

        // sink retries shouldn't be negative
        assertSinkValidationRejects(
                SINK_MAX_RETRIES.key(),
                "-1",
                "The sink max retry times must be larger than or equal to 0.");

        // sink retry interval shouldn't be 0
        assertSinkValidationRejects(
                SINK_RETRY_INTERVAL.key(),
                "0ms",
                "The retry interval (in milliseconds) must be larger than 0.");

        // sink buffered rows should be larger than 0
        assertSinkValidationRejects(
                BUFFER_FLUSH_MAX_ROWS.key(),
                "0",
                "Max number of batch size must be larger than 0.");

        // sink delivery guarantee shouldn't be exactly-once
        assertSinkValidationRejects(
                DELIVERY_GUARANTEE.key(),
                "exactly-once",
                "Mongo sink does not support the EXACTLY_ONCE guarantee.");
    }

    private void assertSourceValidationRejects(String key, String value, String errorMessage) {
        assertThatThrownBy(
                        () -> createTableSource(SCHEMA, getRequiredOptionsWithSetting(key, value)))
                .hasStackTraceContaining(errorMessage);
    }

    private void assertSinkValidationRejects(String key, String value, String errorMessage) {
        assertThatThrownBy(() -> createTableSink(SCHEMA, getRequiredOptionsWithSetting(key, value)))
                .hasStackTraceContaining(errorMessage);
    }

    private static Map<String, String> getRequiredOptionsWithSetting(String key, String value) {
        Map<String, String> requiredOptions = getRequiredOptions();
        requiredOptions.put(key, value);
        return requiredOptions;
    }

    private static Map<String, String> getRequiredOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(CONNECTOR.key(), "mongodb");
        options.put(URI.key(), "mongodb://127.0.0.1:27017");
        options.put(DATABASE.key(), "test_db");
        options.put(COLLECTION.key(), "test_coll");
        return options;
    }

    private static MongoConnectionOptions getConnectionOptions() {
        return MongoConnectionOptions.builder()
                .setUri("mongodb://127.0.0.1:27017")
                .setDatabase("test_db")
                .setCollection("test_coll")
                .build();
    }
}
