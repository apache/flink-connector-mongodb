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

package org.apache.flink.connector.mongodb.sink.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.DeliveryGuarantee;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.DELIVERY_GUARANTEE;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.SINK_MAX_RETRIES;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.SINK_RETRY_INTERVAL;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Configurations for MongoSink to control write operations. All the options list here could be
 * configured by {@link MongoWriteOptionsBuilder}.
 */
@PublicEvolving
public final class MongoWriteOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int batchSize;
    private final long batchIntervalMs;
    private final int maxRetries;
    private final long retryIntervalMs;
    private final DeliveryGuarantee deliveryGuarantee;

    private MongoWriteOptions(
            int batchSize,
            long batchIntervalMs,
            int maxRetries,
            long retryIntervalMs,
            DeliveryGuarantee deliveryGuarantee) {
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;
        this.maxRetries = maxRetries;
        this.retryIntervalMs = retryIntervalMs;
        this.deliveryGuarantee = deliveryGuarantee;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getBatchIntervalMs() {
        return batchIntervalMs;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public long getRetryIntervalMs() {
        return retryIntervalMs;
    }

    public DeliveryGuarantee getDeliveryGuarantee() {
        return deliveryGuarantee;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoWriteOptions that = (MongoWriteOptions) o;
        return batchSize == that.batchSize
                && batchIntervalMs == that.batchIntervalMs
                && maxRetries == that.maxRetries
                && retryIntervalMs == that.retryIntervalMs
                && deliveryGuarantee == that.deliveryGuarantee;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                batchSize, batchIntervalMs, maxRetries, retryIntervalMs, deliveryGuarantee);
    }

    public static MongoWriteOptionsBuilder builder() {
        return new MongoWriteOptionsBuilder();
    }

    /** Builder for {@link MongoWriteOptions}. */
    @PublicEvolving
    public static class MongoWriteOptionsBuilder {
        private int batchSize = BUFFER_FLUSH_MAX_ROWS.defaultValue();
        private long batchIntervalMs = BUFFER_FLUSH_INTERVAL.defaultValue().toMillis();
        private int maxRetries = SINK_MAX_RETRIES.defaultValue();
        private long retryIntervalMs = SINK_RETRY_INTERVAL.defaultValue().toMillis();
        private DeliveryGuarantee deliveryGuarantee = DELIVERY_GUARANTEE.defaultValue();

        private MongoWriteOptionsBuilder() {}

        /**
         * Sets the maximum number of actions to buffer for each batch request. You can pass -1 to
         * disable batching.
         *
         * @param batchSize the maximum number of actions to buffer for each batch request.
         * @return this builder
         */
        public MongoWriteOptionsBuilder setBatchSize(int batchSize) {
            checkArgument(
                    batchSize == -1 || batchSize > 0,
                    "Max number of batch size must be larger than 0.");
            this.batchSize = batchSize;
            return this;
        }

        /**
         * Sets the bulk flush interval, in milliseconds. You can pass -1 to disable it.
         *
         * @param batchIntervalMs the batch flush interval, in milliseconds.
         * @return this builder
         */
        public MongoWriteOptionsBuilder setBatchIntervalMs(long batchIntervalMs) {
            checkArgument(
                    batchIntervalMs == -1 || batchIntervalMs >= 0,
                    "The batch flush interval (in milliseconds) between each flush must be larger than "
                            + "or equal to 0.");
            this.batchIntervalMs = batchIntervalMs;
            return this;
        }

        /**
         * Sets the max retry times if writing records failed.
         *
         * @param maxRetries the max retry times.
         * @return this builder
         */
        public MongoWriteOptionsBuilder setMaxRetries(int maxRetries) {
            checkArgument(
                    maxRetries >= 0, "The sink max retry times must be larger than or equal to 0.");
            this.maxRetries = maxRetries;
            return this;
        }

        /**
         * Sets the retry interval if writing records to database failed.
         *
         * @param retryIntervalMs the retry time interval, in milliseconds.
         * @return this builder
         */
        public MongoWriteOptionsBuilder setRetryIntervalMs(long retryIntervalMs) {
            checkArgument(
                    retryIntervalMs > 0,
                    "The retry interval (in milliseconds) must be larger than 0.");
            this.retryIntervalMs = retryIntervalMs;
            return this;
        }

        /**
         * Sets the wanted {@link DeliveryGuarantee}. The default delivery guarantee is {@link
         * DeliveryGuarantee#AT_LEAST_ONCE}
         *
         * @param deliveryGuarantee which describes the record emission behaviour
         * @return this builder
         */
        public MongoWriteOptionsBuilder setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
            checkArgument(
                    deliveryGuarantee != DeliveryGuarantee.EXACTLY_ONCE,
                    "Mongo sink does not support the EXACTLY_ONCE guarantee.");
            this.deliveryGuarantee = checkNotNull(deliveryGuarantee);
            return this;
        }

        /**
         * Build the {@link MongoWriteOptions}.
         *
         * @return a MongoWriteOptions with the settings made for this builder.
         */
        public MongoWriteOptions build() {
            return new MongoWriteOptions(
                    batchSize, batchIntervalMs, maxRetries, retryIntervalMs, deliveryGuarantee);
        }
    }
}
