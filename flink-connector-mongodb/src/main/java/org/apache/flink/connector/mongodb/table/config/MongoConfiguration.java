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

package org.apache.flink.connector.mongodb.table.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.table.connector.source.lookup.LookupOptions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

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
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;

/** MongoDB configuration. */
@PublicEvolving
public class MongoConfiguration implements Serializable {
    private static final long serialVersionUID = 1L;

    private final ReadableConfig config;

    public MongoConfiguration(ReadableConfig config) {
        this.config = config;
    }

    // -----------------------------------Connection Config----------------------------------------
    public String getUri() {
        return config.get(URI);
    }

    public String getDatabase() {
        return config.get(DATABASE);
    }

    public String getCollection() {
        return config.get(COLLECTION);
    }

    // -----------------------------------Read Config----------------------------------------
    public int getFetchSize() {
        return config.get(SCAN_FETCH_SIZE);
    }

    public int getCursorBatchSize() {
        return config.get(SCAN_CURSOR_BATCH_SIZE);
    }

    public boolean isNoCursorTimeout() {
        return config.get(SCAN_CURSOR_NO_TIMEOUT);
    }

    public PartitionStrategy getPartitionStrategy() {
        return config.get(SCAN_PARTITION_STRATEGY);
    }

    public MemorySize getPartitionSize() {
        return config.get(SCAN_PARTITION_SIZE);
    }

    public int getSamplesPerPartition() {
        return config.get(SCAN_PARTITION_SAMPLES);
    }

    // -----------------------------------Lookup Config----------------------------------------
    public int getLookupMaxRetries() {
        return config.get(LookupOptions.MAX_RETRIES);
    }

    public long getLookupRetryIntervalMs() {
        return config.get(LOOKUP_RETRY_INTERVAL).toMillis();
    }

    // -----------------------------------Write Config------------------------------------------
    public int getBufferFlushMaxRows() {
        return config.get(BUFFER_FLUSH_MAX_ROWS);
    }

    public long getBufferFlushIntervalMs() {
        return config.get(BUFFER_FLUSH_INTERVAL).toMillis();
    }

    public int getSinkMaxRetries() {
        return config.get(SINK_MAX_RETRIES);
    }

    public long getSinkRetryIntervalMs() {
        return config.get(SINK_RETRY_INTERVAL).toMillis();
    }

    public DeliveryGuarantee getDeliveryGuarantee() {
        return config.get(DELIVERY_GUARANTEE);
    }

    @Nullable
    public Integer getSinkParallelism() {
        return config.getOptional(SINK_PARALLELISM).orElse(null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoConfiguration that = (MongoConfiguration) o;
        return Objects.equals(config, that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config);
    }
}
