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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.mongodb.common.config.MongoConnectionOptions;
import org.apache.flink.connector.mongodb.sink.config.MongoWriteOptions;
import org.apache.flink.connector.mongodb.source.config.MongoReadOptions;
import org.apache.flink.connector.mongodb.table.config.MongoConfiguration;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.COLLECTION;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.DATABASE;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.DELIVERY_GUARANTEE;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.FILTER_HANDLING_POLICY;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.LOOKUP_RETRY_INTERVAL;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.SCAN_CURSOR_NO_TIMEOUT;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.SCAN_FETCH_SIZE;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.SCAN_PARTITION_RECORD_SIZE;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.SCAN_PARTITION_SAMPLES;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.SCAN_PARTITION_SIZE;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.SCAN_PARTITION_STRATEGY;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.SINK_MAX_RETRIES;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.SINK_RETRY_INTERVAL;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.URI;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;

/**
 * Factory for creating configured instances of {@link MongoDynamicTableSource} and {@link
 * MongoDynamicTableSink}.
 */
@Internal
public class MongoDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "mongodb";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URI);
        requiredOptions.add(DATABASE);
        requiredOptions.add(COLLECTION);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(SCAN_FETCH_SIZE);
        optionalOptions.add(SCAN_CURSOR_NO_TIMEOUT);
        optionalOptions.add(SCAN_PARTITION_STRATEGY);
        optionalOptions.add(SCAN_PARTITION_SIZE);
        optionalOptions.add(SCAN_PARTITION_SAMPLES);
        optionalOptions.add(SCAN_PARTITION_RECORD_SIZE);
        optionalOptions.add(BUFFER_FLUSH_MAX_ROWS);
        optionalOptions.add(BUFFER_FLUSH_INTERVAL);
        optionalOptions.add(DELIVERY_GUARANTEE);
        optionalOptions.add(SINK_MAX_RETRIES);
        optionalOptions.add(SINK_RETRY_INTERVAL);
        optionalOptions.add(SINK_PARALLELISM);
        optionalOptions.add(LookupOptions.CACHE_TYPE);
        optionalOptions.add(LookupOptions.MAX_RETRIES);
        optionalOptions.add(LOOKUP_RETRY_INTERVAL);
        optionalOptions.add(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS);
        optionalOptions.add(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE);
        optionalOptions.add(LookupOptions.PARTIAL_CACHE_MAX_ROWS);
        optionalOptions.add(LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY);
        optionalOptions.add(FILTER_HANDLING_POLICY);
        return optionalOptions;
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        final Set<ConfigOption<?>> forwardOptions = new HashSet<>();
        forwardOptions.add(URI);
        forwardOptions.add(DATABASE);
        forwardOptions.add(COLLECTION);
        forwardOptions.add(SCAN_FETCH_SIZE);
        forwardOptions.add(SCAN_CURSOR_NO_TIMEOUT);
        forwardOptions.add(BUFFER_FLUSH_MAX_ROWS);
        forwardOptions.add(BUFFER_FLUSH_INTERVAL);
        forwardOptions.add(SINK_MAX_RETRIES);
        forwardOptions.add(SINK_RETRY_INTERVAL);
        return forwardOptions;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig options = helper.getOptions();
        MongoConfiguration config = new MongoConfiguration(options);
        helper.validate();

        return new MongoDynamicTableSource(
                getConnectionOptions(config),
                getReadOptions(config),
                getLookupCache(options),
                config.getLookupMaxRetries(),
                config.getLookupRetryIntervalMs(),
                config.getFilterHandlingPolicy(),
                context.getPhysicalRowDataType());
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        MongoConfiguration config = new MongoConfiguration(helper.getOptions());
        helper.validate();

        return new MongoDynamicTableSink(
                getConnectionOptions(config),
                getWriteOptions(config),
                config.getSinkParallelism(),
                context.getCatalogTable().getResolvedSchema(),
                context.getCatalogTable().getPartitionKeys().toArray(new String[0]));
    }

    @Nullable
    private LookupCache getLookupCache(ReadableConfig tableOptions) {
        LookupCache cache = null;
        if (tableOptions
                .get(LookupOptions.CACHE_TYPE)
                .equals(LookupOptions.LookupCacheType.PARTIAL)) {
            cache = DefaultLookupCache.fromConfig(tableOptions);
        }
        return cache;
    }

    private static MongoConnectionOptions getConnectionOptions(MongoConfiguration configuration) {
        return MongoConnectionOptions.builder()
                .setUri(configuration.getUri())
                .setDatabase(configuration.getDatabase())
                .setCollection(configuration.getCollection())
                .build();
    }

    private static MongoReadOptions getReadOptions(MongoConfiguration configuration) {
        return MongoReadOptions.builder()
                .setFetchSize(configuration.getFetchSize())
                .setNoCursorTimeout(configuration.isNoCursorTimeout())
                .setPartitionStrategy(configuration.getPartitionStrategy())
                .setPartitionSize(configuration.getPartitionSize())
                .setPartitionRecordSize(configuration.getPartitionRecordSize())
                .setSamplesPerPartition(configuration.getSamplesPerPartition())
                .build();
    }

    private static MongoWriteOptions getWriteOptions(MongoConfiguration configuration) {
        return MongoWriteOptions.builder()
                .setBatchSize(configuration.getBufferFlushMaxRows())
                .setBatchIntervalMs(configuration.getBufferFlushIntervalMs())
                .setMaxRetries(configuration.getSinkMaxRetries())
                .setRetryIntervalMs(configuration.getSinkRetryIntervalMs())
                .setDeliveryGuarantee(configuration.getDeliveryGuarantee())
                .build();
    }
}
