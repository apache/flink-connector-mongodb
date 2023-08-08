/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.connector.mongodb.source.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.Boundedness;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The startup options for MongoDB source. */
@PublicEvolving
public class MongoStartupOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final StartupMode startupMode;
    @Nullable private final Long startupTimestampMillis;

    private MongoStartupOptions(StartupMode startupMode, @Nullable Long startupTimestampMillis) {
        this.startupMode = checkNotNull(startupMode);
        this.startupTimestampMillis = startupTimestampMillis;

        switch (startupMode) {
            case BOUNDED:
            case INITIAL:
            case LATEST_OFFSET:
                break;
            case TIMESTAMP:
                checkNotNull(startupTimestampMillis);
                break;
            default:
                throw new UnsupportedOperationException(startupMode + " mode is not supported.");
        }
    }

    /** The boundedness of startup mode. * */
    public Boundedness boundedness() {
        if (startupMode == StartupMode.BOUNDED) {
            return Boundedness.BOUNDED;
        } else {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }
    }

    public StartupMode getStartupMode() {
        return startupMode;
    }

    @Nullable
    public Long getStartupTimestampMillis() {
        return startupTimestampMillis;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoStartupOptions that = (MongoStartupOptions) o;
        return Objects.equals(startupMode, that.startupMode)
                && Objects.equals(startupTimestampMillis, that.startupTimestampMillis);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startupMode, startupTimestampMillis);
    }

    /** Just performs a snapshot on the specified collections upon first startup. */
    public static MongoStartupOptions bounded() {
        return new MongoStartupOptions(StartupMode.BOUNDED, null);
    }

    /**
     * Performs an initial snapshot on the specified collections upon first startup, and continue to
     * read the latest change log.
     */
    public static MongoStartupOptions initial() {
        return new MongoStartupOptions(StartupMode.INITIAL, null);
    }

    /**
     * Never to perform snapshot on the specified collections upon first startup, just read from the
     * end of the change log which means only have the changes since the connector was started.
     */
    public static MongoStartupOptions latest() {
        return new MongoStartupOptions(StartupMode.LATEST_OFFSET, null);
    }

    /**
     * Never to perform snapshot on the specified collections upon first startup, and directly read
     * change log from the specified timestamp.
     *
     * @param startupTimestampMillis timestamp for the startup offsets, as milliseconds from epoch.
     */
    public static MongoStartupOptions timestamp(long startupTimestampMillis) {
        return new MongoStartupOptions(StartupMode.TIMESTAMP, startupTimestampMillis);
    }

    /** The startup mode for MongoDB source. */
    @PublicEvolving
    public enum StartupMode {
        BOUNDED,
        INITIAL,
        LATEST_OFFSET,
        TIMESTAMP
    }
}
