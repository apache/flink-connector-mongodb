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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.mongodb.common.config.MongoConnectionOptions;
import org.apache.flink.connector.mongodb.source.MongoSource;
import org.apache.flink.connector.mongodb.source.config.MongoReadOptions;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.connector.mongodb.table.serialization.MongoRowDataDeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import com.mongodb.client.model.Filters;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.LOOKUP_RETRY_INTERVAL;
import static org.apache.flink.util.Preconditions.checkArgument;

/** A {@link DynamicTableSource} for MongoDB. */
@Internal
public class MongoDynamicTableSource
        implements ScanTableSource,
                LookupTableSource,
                SupportsProjectionPushDown,
                SupportsFilterPushDown,
                SupportsLimitPushDown {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDynamicTableSource.class);

    private final MongoConnectionOptions connectionOptions;
    private final MongoReadOptions readOptions;
    @Nullable private final LookupCache lookupCache;
    private final int lookupMaxRetries;
    private final long lookupRetryIntervalMs;
    private DataType producedDataType;
    private int limit = -1;

    private BsonDocument filter = Filters.empty().toBsonDocument();

    public MongoDynamicTableSource(
            MongoConnectionOptions connectionOptions,
            MongoReadOptions readOptions,
            @Nullable LookupCache lookupCache,
            int lookupMaxRetries,
            long lookupRetryIntervalMs,
            DataType producedDataType) {
        this.connectionOptions = connectionOptions;
        this.readOptions = readOptions;
        this.lookupCache = lookupCache;
        checkArgument(
                lookupMaxRetries >= 0,
                String.format(
                        "The '%s' must be larger than or equal to 0.",
                        LookupOptions.MAX_RETRIES.key()));
        checkArgument(
                lookupRetryIntervalMs > 0,
                String.format("The '%s' must be larger than 0.", LOOKUP_RETRY_INTERVAL.key()));
        this.lookupMaxRetries = lookupMaxRetries;
        this.lookupRetryIntervalMs = lookupRetryIntervalMs;
        this.producedDataType = producedDataType;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        final List<String> keyNames = new ArrayList<>(context.getKeys().length);
        for (int[] innerKeyArr : context.getKeys()) {
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "MongoDB only support non-nested look up keys yet");
            keyNames.add(DataType.getFieldNames(producedDataType).get(innerKeyArr[0]));
        }
        final RowType rowType = (RowType) producedDataType.getLogicalType();

        MongoRowDataLookupFunction lookupFunction =
                new MongoRowDataLookupFunction(
                        connectionOptions,
                        lookupMaxRetries,
                        lookupRetryIntervalMs,
                        DataType.getFieldNames(producedDataType),
                        DataType.getFieldDataTypes(producedDataType),
                        keyNames,
                        rowType);
        if (lookupCache != null) {
            return PartialCachingLookupProvider.of(lookupFunction, lookupCache);
        } else {
            return LookupFunctionProvider.of(lookupFunction);
        }
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final RowType rowType = (RowType) producedDataType.getLogicalType();
        final TypeInformation<RowData> typeInfo =
                runtimeProviderContext.createTypeInformation(producedDataType);

        final MongoDeserializationSchema<RowData> deserializationSchema =
                new MongoRowDataDeserializationSchema(rowType, typeInfo);

        MongoSource<RowData> mongoSource =
                MongoSource.<RowData>builder()
                        .setUri(connectionOptions.getUri())
                        .setDatabase(connectionOptions.getDatabase())
                        .setCollection(connectionOptions.getCollection())
                        .setFetchSize(readOptions.getFetchSize())
                        .setNoCursorTimeout(readOptions.isNoCursorTimeout())
                        .setPartitionStrategy(readOptions.getPartitionStrategy())
                        .setPartitionSize(readOptions.getPartitionSize())
                        .setSamplesPerPartition(readOptions.getSamplesPerPartition())
                        .setLimit(limit)
                        .setProjectedFields(DataType.getFieldNames(producedDataType))
                        .setFilter(filter)
                        .setDeserializationSchema(deserializationSchema)
                        .build();

        return SourceProvider.of(mongoSource);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public DynamicTableSource copy() {
        MongoDynamicTableSource newSource =
                new MongoDynamicTableSource(
                        connectionOptions,
                        readOptions,
                        lookupCache,
                        lookupMaxRetries,
                        lookupRetryIntervalMs,
                        producedDataType);
        newSource.filter = BsonDocument.parse(filter.toJson());
        return newSource;
    }

    @Override
    public String asSummaryString() {
        return "MongoDB";
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = (int) limit;
    }

    @Override
    public boolean supportsNestedProjection() {
        // planner doesn't support nested projection push down yet.
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.producedDataType = producedDataType;
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        List<ResolvedExpression> acceptedFilters = new ArrayList<>();
        List<ResolvedExpression> remainingFilters = new ArrayList<>();

        List<Bson> mongoFilters = new ArrayList<>();
        for (ResolvedExpression filter : filters) {
            BsonDocument simpleFilter = parseFilter(filter);
            if (simpleFilter.isEmpty()) {
                remainingFilters.add(filter);
            } else {
                acceptedFilters.add(filter);
                mongoFilters.add(simpleFilter);
            }
        }

        if (!mongoFilters.isEmpty()) {
            Bson mergedFilter =
                    mongoFilters.size() == 1 ? mongoFilters.get(0) : Filters.and(mongoFilters);
            this.filter = mergedFilter.toBsonDocument();
            LOG.info("Pushed down filters: {}", filter.toJson());
        }

        return Result.of(acceptedFilters, remainingFilters);
    }

    private BsonDocument parseFilter(ResolvedExpression filter) {
        if (filter instanceof CallExpression) {
            CallExpression callExp = (CallExpression) filter;
            return MongoFilterPushDownVisitor.INSTANCE.visit(callExp);
        } else {
            return Filters.empty().toBsonDocument();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MongoDynamicTableSource)) {
            return false;
        }
        MongoDynamicTableSource that = (MongoDynamicTableSource) o;
        return Objects.equals(connectionOptions, that.connectionOptions)
                && Objects.equals(readOptions, that.readOptions)
                && Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(limit, that.limit)
                && Objects.equals(filter, that.filter)
                && Objects.equals(lookupCache, that.lookupCache)
                && Objects.equals(lookupMaxRetries, that.lookupMaxRetries)
                && Objects.equals(lookupRetryIntervalMs, that.lookupRetryIntervalMs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                connectionOptions,
                readOptions,
                producedDataType,
                limit,
                filter,
                lookupCache,
                lookupMaxRetries,
                lookupRetryIntervalMs);
    }
}
