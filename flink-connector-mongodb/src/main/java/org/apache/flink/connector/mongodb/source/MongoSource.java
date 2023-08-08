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

package org.apache.flink.connector.mongodb.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.mongodb.common.config.MongoConnectionOptions;
import org.apache.flink.connector.mongodb.source.config.MongoChangeStreamOptions;
import org.apache.flink.connector.mongodb.source.config.MongoReadOptions;
import org.apache.flink.connector.mongodb.source.config.MongoStartupOptions;
import org.apache.flink.connector.mongodb.source.enumerator.MongoSourceEnumState;
import org.apache.flink.connector.mongodb.source.enumerator.MongoSourceEnumStateSerializer;
import org.apache.flink.connector.mongodb.source.enumerator.MongoSourceEnumerator;
import org.apache.flink.connector.mongodb.source.enumerator.assigner.MongoHybridSplitAssigner;
import org.apache.flink.connector.mongodb.source.enumerator.assigner.MongoScanSplitAssigner;
import org.apache.flink.connector.mongodb.source.enumerator.assigner.MongoSplitAssigner;
import org.apache.flink.connector.mongodb.source.enumerator.assigner.MongoStreamSplitAssigner;
import org.apache.flink.connector.mongodb.source.reader.MongoSourceReader;
import org.apache.flink.connector.mongodb.source.reader.MongoSourceReaderContext;
import org.apache.flink.connector.mongodb.source.reader.MongoSourceRecord;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.connector.mongodb.source.reader.emitter.MongoRecordEmitter;
import org.apache.flink.connector.mongodb.source.reader.split.MongoHybridSourceSplitReader;
import org.apache.flink.connector.mongodb.source.split.MongoSourceSplit;
import org.apache.flink.connector.mongodb.source.split.MongoSourceSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.api.ValidationException;

import javax.annotation.Nullable;

import java.util.List;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The Source implementation of MongoDB. Use a {@link MongoSourceBuilder} to construct a {@link
 * MongoSource}. The following example shows how to create a MongoSource emitting records of <code>
 * String</code> type.
 *
 * <pre>{@code
 * MongoSource<String> source = MongoSource.<String>builder()
 *      .setUri("mongodb://user:password@127.0.0.1:27017")
 *      .setDatabase("db")
 *      .setCollection("coll")
 *      .setDeserializationSchema(new MongoJsonDeserializationSchema())
 *      .build();
 * }</pre>
 *
 * <p>See {@link MongoSourceBuilder} for more details.
 *
 * @param <OUT> The output type of the source.
 */
@PublicEvolving
public class MongoSource<OUT>
        implements Source<OUT, MongoSourceSplit, MongoSourceEnumState>, ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    /** The connection options for MongoDB source. */
    private final MongoConnectionOptions connectionOptions;

    /** The read options for MongoDB source. */
    private final MongoReadOptions readOptions;

    /** The change stream options for MongoDB source. */
    private final MongoChangeStreamOptions changeStreamOptions;

    /** The startup options for MongoDB source. */
    private final MongoStartupOptions startupOptions;

    /** The projections for MongoDB source. */
    @Nullable private final List<String> projectedFields;

    /** The limit for MongoDB source. */
    private final int limit;

    /** The mongo deserialization schema used for deserializing message. */
    private final MongoDeserializationSchema<OUT> deserializationSchema;

    MongoSource(
            MongoConnectionOptions connectionOptions,
            MongoReadOptions readOptions,
            MongoChangeStreamOptions changeStreamOptions,
            MongoStartupOptions startupOptions,
            @Nullable List<String> projectedFields,
            int limit,
            MongoDeserializationSchema<OUT> deserializationSchema) {
        this.connectionOptions = checkNotNull(connectionOptions);
        this.readOptions = checkNotNull(readOptions);
        this.changeStreamOptions = checkNotNull(changeStreamOptions);
        this.startupOptions = checkNotNull(startupOptions);
        this.projectedFields = projectedFields;
        this.limit = limit;
        this.deserializationSchema = checkNotNull(deserializationSchema);
    }

    /**
     * Get a MongoSourceBuilder to builder a {@link MongoSource}.
     *
     * @return a Mongo source builder.
     */
    public static <OUT> MongoSourceBuilder<OUT> builder() {
        return new MongoSourceBuilder<>();
    }

    @Override
    public Boundedness getBoundedness() {
        return startupOptions.boundedness();
    }

    @Override
    public SourceReader<OUT, MongoSourceSplit> createReader(SourceReaderContext readerContext) {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<MongoSourceRecord>> elementsQueue =
                new FutureCompletingBlockingQueue<>();

        MongoSourceReaderContext mongoReaderContext =
                new MongoSourceReaderContext(readerContext, limit);

        Supplier<SplitReader<MongoSourceRecord, MongoSourceSplit>> splitReaderSupplier =
                () ->
                        new MongoHybridSourceSplitReader(
                                connectionOptions,
                                readOptions,
                                changeStreamOptions,
                                mongoReaderContext,
                                projectedFields);

        return new MongoSourceReader<>(
                elementsQueue,
                splitReaderSupplier,
                new MongoRecordEmitter<>(deserializationSchema),
                mongoReaderContext);
    }

    @Override
    public SplitEnumerator<MongoSourceSplit, MongoSourceEnumState> createEnumerator(
            SplitEnumeratorContext<MongoSourceSplit> enumContext) {
        MongoSourceEnumState initialState = MongoSourceEnumState.initialState();
        return new MongoSourceEnumerator(
                startupOptions.boundedness(), enumContext, createMongoSplitAssigner(initialState));
    }

    @Override
    public SplitEnumerator<MongoSourceSplit, MongoSourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<MongoSourceSplit> enumContext, MongoSourceEnumState checkpoint) {
        return new MongoSourceEnumerator(
                startupOptions.boundedness(), enumContext, createMongoSplitAssigner(checkpoint));
    }

    @Override
    public SimpleVersionedSerializer<MongoSourceSplit> getSplitSerializer() {
        return MongoSourceSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<MongoSourceEnumState> getEnumeratorCheckpointSerializer() {
        return MongoSourceEnumStateSerializer.INSTANCE;
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    private MongoSplitAssigner createMongoSplitAssigner(MongoSourceEnumState sourceEnumState) {
        MongoSplitAssigner splitAssigner;
        switch (startupOptions.getStartupMode()) {
            case BOUNDED:
                splitAssigner =
                        new MongoScanSplitAssigner(connectionOptions, readOptions, sourceEnumState);
                break;
            case LATEST_OFFSET:
            case TIMESTAMP:
                splitAssigner = new MongoStreamSplitAssigner(connectionOptions, startupOptions);
                break;
            case INITIAL:
                splitAssigner =
                        new MongoHybridSplitAssigner(
                                connectionOptions, readOptions, sourceEnumState);
                break;
            default:
                throw new ValidationException(
                        "Unsupported startup mode " + startupOptions.getStartupMode());
        }
        return splitAssigner;
    }
}
