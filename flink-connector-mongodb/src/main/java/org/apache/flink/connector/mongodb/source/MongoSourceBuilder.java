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
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.mongodb.common.config.MongoConnectionOptions;
import org.apache.flink.connector.mongodb.source.config.MongoChangeStreamOptions;
import org.apache.flink.connector.mongodb.source.config.MongoReadOptions;
import org.apache.flink.connector.mongodb.source.config.MongoStartupOptions;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.connector.mongodb.source.reader.split.MongoScanSourceSplitReader;

import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import org.bson.BsonDocument;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.CollectionUtil.isNullOrEmpty;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The builder class for {@link MongoSource} to make it easier for the users to construct a {@link
 * MongoSource}.
 *
 * @param <OUT> The output type of the source.
 */
@PublicEvolving
public class MongoSourceBuilder<OUT> {

    private final MongoConnectionOptions.MongoConnectionOptionsBuilder connectionOptionsBuilder;
    private final MongoReadOptions.MongoReadOptionsBuilder readOptionsBuilder;
    private final MongoChangeStreamOptions.MongoChangeStreamOptionsBuilder
            changeStreamOptionsBuilder;

    private MongoStartupOptions startupOptions = MongoStartupOptions.bounded();
    private List<String> projectedFields;
    private int limit = -1;
    private MongoDeserializationSchema<OUT> deserializationSchema;

    MongoSourceBuilder() {
        this.connectionOptionsBuilder = MongoConnectionOptions.builder();
        this.readOptionsBuilder = MongoReadOptions.builder();
        this.changeStreamOptionsBuilder = MongoChangeStreamOptions.builder();
    }

    /**
     * Sets the connection string of MongoDB.
     *
     * @param uri connection string of MongoDB
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setUri(String uri) {
        connectionOptionsBuilder.setUri(uri);
        return this;
    }

    /**
     * Sets the database to sink of MongoDB.
     *
     * @param database the database to read from MongoDB.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setDatabase(String database) {
        connectionOptionsBuilder.setDatabase(database);
        return this;
    }

    /**
     * Sets the collection to sink of MongoDB.
     *
     * @param collection the collection to read from MongoDB.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setCollection(String collection) {
        connectionOptionsBuilder.setCollection(collection);
        return this;
    }

    /**
     * Specifies the startup options.
     *
     * @param startupOptions the connector startup options {@link MongoStartupOptions}.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setStartupOptions(MongoStartupOptions startupOptions) {
        this.startupOptions = startupOptions;
        return this;
    }

    /**
     * Sets the number of documents should be fetched per round-trip when reading.
     *
     * @param fetchSize the number of documents should be fetched per round-trip when reading.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setFetchSize(int fetchSize) {
        readOptionsBuilder.setFetchSize(fetchSize);
        return this;
    }

    /**
     * The MongoDB server normally times out idle cursors after an inactivity period (10 minutes) to
     * prevent excess memory use. Set this option to prevent that. If a session is idle for longer
     * than 30 minutes, the MongoDB server marks that session as expired and may close it at any
     * time. When the MongoDB server closes the session, it also kills any in-progress operations
     * and open cursors associated with the session. This includes cursors configured with {@code
     * noCursorTimeout()} or a {@code maxTimeMS()} greater than 30 minutes.
     *
     * @param noCursorTimeout Set this option to true to prevent cursor timeout (10 minutes)
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setNoCursorTimeout(boolean noCursorTimeout) {
        readOptionsBuilder.setNoCursorTimeout(noCursorTimeout);
        return this;
    }

    /**
     * Sets the partition strategy. Available partition strategies are single, sample, split-vector,
     * sharded and default. You can see {@link PartitionStrategy} for detail.
     *
     * @param partitionStrategy the strategy of a partition.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setPartitionStrategy(PartitionStrategy partitionStrategy) {
        readOptionsBuilder.setPartitionStrategy(partitionStrategy);
        return this;
    }

    /**
     * Sets the partition memory size of MongoDB split. Split a MongoDB collection into multiple
     * partitions according to the partition memory size. Partitions can be read in parallel by
     * multiple {@link MongoScanSourceSplitReader} to speed up the overall read time.
     *
     * @param partitionSize the memory size of a partition.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setPartitionSize(MemorySize partitionSize) {
        readOptionsBuilder.setPartitionSize(partitionSize);
        return this;
    }

    /**
     * Sets the number of samples to take per partition which is only used for the sample partition
     * strategy {@link PartitionStrategy#SAMPLE}. The sample partitioner samples the collection,
     * projects and sorts by the partition fields. Then uses every {@code samplesPerPartition} as
     * the value to use to calculate the partition boundaries. The total number of samples taken is:
     * samples per partition * ( count of documents / number of documents per partition).
     *
     * @param samplesPerPartition number of samples per partition.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setSamplesPerPartition(int samplesPerPartition) {
        readOptionsBuilder.setSamplesPerPartition(samplesPerPartition);
        return this;
    }

    /**
     * Sets the limit of documents to read. If limit is not set or set to -1, the documents of the
     * entire collection will be read.
     *
     * @param limit the limit of documents to read.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setLimit(int limit) {
        checkArgument(limit == -1 || limit > 0, "The limit must be larger than 0");
        this.limit = limit;
        return this;
    }

    /**
     * Sets the projection fields of documents to read.
     *
     * @param projectedFields the projection fields of documents to read.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setProjectedFields(String... projectedFields) {
        checkNotNull(projectedFields, "The projected fields must be supplied");
        return setProjectedFields(Arrays.asList(projectedFields));
    }

    /**
     * Sets the projection fields of documents to read.
     *
     * @param projectedFields the projection fields of documents to read.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setProjectedFields(List<String> projectedFields) {
        checkArgument(
                !isNullOrEmpty(projectedFields), "At least one projected field to be supplied");
        this.projectedFields = projectedFields;
        return this;
    }

    /**
     * Sets the number of change stream documents should be fetched per round-trip when reading.
     *
     * @param changeStreamFetchSize the number of change stream documents should be fetched per
     *     round-trip when reading.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setChangeStreamFetchSize(int changeStreamFetchSize) {
        changeStreamOptionsBuilder.setFetchSize(changeStreamFetchSize);
        return this;
    }

    /**
     * Determines what values your change stream returns on update operations. The default setting
     * returns the differences between the original document and the updated document. The
     * updateLookup setting returns the differences between the original document and updated
     * document as well as a copy of the entire updated document at a point in time after the
     * update. The whenAvailable setting returns the updated document, if available. The required
     * setting returns the updated document and raises an error if it is not available.
     *
     * @param fullDocument the values your change stream returns on update operations.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setFullDocument(FullDocument fullDocument) {
        changeStreamOptionsBuilder.setFullDocument(fullDocument);
        return this;
    }

    /**
     * Configures the document pre-image your change stream returns on update operations. The
     * pre-image is not available for source records published while copying existing data, and the
     * pre-image configuration has no effect on copying. The default setting suppresses the document
     * pre-image. The whenAvailable setting returns the document pre-image if it's available, before
     * it was replaced, updated, or deleted. The required setting returns the document pre-image and
     * raises an error if it is not available.
     *
     * @param fullDocumentBeforeChange the document pre-image your change stream returns on update
     *     operations.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setFullDocumentBeforeChange(
            FullDocumentBeforeChange fullDocumentBeforeChange) {
        changeStreamOptionsBuilder.setFullDocumentBeforeChange(fullDocumentBeforeChange);
        return this;
    }

    /**
     * Sets the deserialization schema for MongoDB {@link BsonDocument}.
     *
     * @param deserializationSchema the deserialization schema to deserialize {@link BsonDocument}.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setDeserializationSchema(
            MongoDeserializationSchema<OUT> deserializationSchema) {
        checkNotNull(deserializationSchema, "The deserialization schema must not be null");
        this.deserializationSchema = deserializationSchema;
        return this;
    }

    /**
     * Build the {@link MongoSource}.
     *
     * @return a MongoSource with the settings made for this builder.
     */
    public MongoSource<OUT> build() {
        checkNotNull(deserializationSchema, "The deserialization schema must be supplied");
        return new MongoSource<>(
                connectionOptionsBuilder.build(),
                readOptionsBuilder.build(),
                changeStreamOptionsBuilder.build(),
                startupOptions,
                projectedFields,
                limit,
                deserializationSchema);
    }
}
