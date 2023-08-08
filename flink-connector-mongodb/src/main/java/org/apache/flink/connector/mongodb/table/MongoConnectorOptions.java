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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.connector.mongodb.table.config.FullDocumentStrategy;

import java.time.Duration;

import static org.apache.flink.configuration.description.TextElement.text;

/**
 * Base options for the MongoDB connector. Needs to be public so that the {@link
 * org.apache.flink.table.api.TableDescriptor} can access it.
 */
@PublicEvolving
public class MongoConnectorOptions {

    private MongoConnectorOptions() {}

    public static final ConfigOption<String> URI =
            ConfigOptions.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies the connection uri of MongoDB.");

    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies the database to read or write of MongoDB.");

    public static final ConfigOption<String> COLLECTION =
            ConfigOptions.key("collection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies the collection to read or write of MongoDB.");

    public static final ConfigOption<Integer> SCAN_FETCH_SIZE =
            ConfigOptions.key("scan.fetch-size")
                    .intType()
                    .defaultValue(2048)
                    .withDescription(
                            "Gives the reader a hint as to the number of documents that should be fetched from the database per round-trip when reading. ");

    public static final ConfigOption<Boolean> SCAN_CURSOR_NO_TIMEOUT =
            ConfigOptions.key("scan.cursor.no-timeout")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "The server normally times out idle cursors after an inactivity"
                                    + " period (10 minutes) to prevent excess memory use. Set this option to true to prevent that."
                                    + " However, if the application takes longer than 30 minutes to process the current batch of documents,"
                                    + " the session is marked as expired and closed.");

    public static final ConfigOption<PartitionStrategy> SCAN_PARTITION_STRATEGY =
            ConfigOptions.key("scan.partition.strategy")
                    .enumType(PartitionStrategy.class)
                    .defaultValue(PartitionStrategy.DEFAULT)
                    .withDescription(
                            "Specifies the partition strategy. Available strategies are single, sample, split-vector, sharded and default."
                                    + "The single partition strategy treats the entire collection as a single partition."
                                    + "The sample partition strategy samples the collection and generate partitions which is fast but possibly uneven."
                                    + "The split-vector partition strategy uses the splitVector command to generate partitions for non-sharded collections which is fast and even. The splitVector permission is required."
                                    + "The sharded partition strategy reads config.chunks (MongoDB splits a sharded collection into chunks, and the range of the chunks are stored within the collection) as the partitions directly."
                                    + "The sharded partition strategy is only used for sharded collection which is fast and even. Read permission of config database is required."
                                    + "The default partition strategy uses sharded strategy for sharded collections otherwise using split vector strategy.");

    public static final ConfigOption<MemorySize> SCAN_PARTITION_SIZE =
            ConfigOptions.key("scan.partition.size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64mb"))
                    .withDescription("Specifies the partition memory size.");

    public static final ConfigOption<Integer> SCAN_PARTITION_SAMPLES =
            ConfigOptions.key("scan.partition.samples")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "Specifies the samples count per partition. It only takes effect when the partition strategy is sample. "
                                    + "The sample partitioner samples the collection, projects and sorts by the partition fields. "
                                    + "Then uses every 'scan.partition.samples' as the value to use to calculate the partition boundaries."
                                    + "The total number of samples taken is calculated as: samples per partition * (count of documents / number of documents per partition.");

    public static final ConfigOption<ScanStartupMode> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .enumType(ScanStartupMode.class)
                    .defaultValue(ScanStartupMode.BOUNDED)
                    .withDescription(
                            "Startup mode for MongoDB connector. valid enumerations are 'bounded', 'initial', 'latest-offset', and 'timestamp'. "
                                    + "bounded: bounded read collection snapshot data. "
                                    + "initial: read collection snapshot data and then continuously read changed data. "
                                    + "latest-offset: continuously read changed data from latest offset of oplog. "
                                    + "timestamp: continuously read changed data from specified timestamp offset of oplog.");

    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS =
            ConfigOptions.key("scan.startup.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Optional timestamp used in case of 'timestamp' startup mode");

    public static final ConfigOption<Duration> LOOKUP_RETRY_INTERVAL =
            ConfigOptions.key("lookup.retry.interval")
                    .durationType()
                    .defaultValue(Duration.ofMillis(1000L))
                    .withDescription(
                            "Specifies the retry time interval if lookup records from database failed.");

    public static final ConfigOption<Integer> CHANGE_STREAM_FETCH_SIZE =
            ConfigOptions.key("change-stream.fetch-size")
                    .intType()
                    .defaultValue(2048)
                    .withDescription(
                            "Gives the reader a hint as to the number of change stream documents that should be fetched from the database per round-trip when reading. ");

    public static final ConfigOption<FullDocumentStrategy> CHANGE_STREAM_FULL_DOCUMENT_STRATEGY =
            ConfigOptions.key("change-stream.full-document.strategy")
                    .enumType(FullDocumentStrategy.class)
                    .defaultValue(FullDocumentStrategy.UPDATE_LOOKUP)
                    .withDescription(
                            "Specifies the full document strategy. Available strategies are update-lookup, and pre-and-post-images."
                                    + "update-lookup: prior to version 6.0 of MongoDB, the pre images was not stored in the oplog. To obtain complete updated row data, use updateLookup feature to lookup the latest snapshot when the oplog record is accessed."
                                    + "pre-and-post-images: starting in MongoDB 6.0, you can use change stream events to output the version of a document before and after changes (the document pre- and post-images).");

    public static final ConfigOption<Integer> BUFFER_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.buffer-flush.max-rows")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Specifies the maximum number of buffered rows per batch request.");

    public static final ConfigOption<Duration> BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("sink.buffer-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("Specifies the batch flush interval.");

    public static final ConfigOption<DeliveryGuarantee> DELIVERY_GUARANTEE =
            ConfigOptions.key("sink.delivery-guarantee")
                    .enumType(DeliveryGuarantee.class)
                    .defaultValue(DeliveryGuarantee.AT_LEAST_ONCE)
                    .withDescription(
                            "Optional delivery guarantee when committing. The exactly-once guarantee is not supported yet.");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Specifies the max retry times if writing records to database failed.");

    public static final ConfigOption<Duration> SINK_RETRY_INTERVAL =
            ConfigOptions.key("sink.retry.interval")
                    .durationType()
                    .defaultValue(Duration.ofMillis(1000L))
                    .withDescription(
                            "Specifies the retry time interval if writing records to database failed.");

    /** Startup mode for the MongoDB connector, see {@link #SCAN_STARTUP_MODE}. */
    public enum ScanStartupMode implements DescribedEnum {
        BOUNDED("bounded", text("Bounded read collection snapshot.")),
        INITIAL("initial", text("Read collection snapshot and continuously read changed data.")),
        LATEST_OFFSET(
                "latest-offset",
                text("Continuously read changed data from latest offset of oplog.")),
        TIMESTAMP(
                "timestamp",
                text("Continuously read changed data from specified timestamp offset of oplog."));

        private final String value;
        private final InlineElement description;

        ScanStartupMode(String value, InlineElement description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return description;
        }
    }
}
