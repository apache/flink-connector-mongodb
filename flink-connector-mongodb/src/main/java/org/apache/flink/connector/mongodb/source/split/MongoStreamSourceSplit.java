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

package org.apache.flink.connector.mongodb.source.split;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SourceSplit;

import java.util.Objects;

/** A {@link SourceSplit} implementation for a MongoDB's change streams. */
@PublicEvolving
public class MongoStreamSourceSplit extends MongoSourceSplit {

    private static final long serialVersionUID = 1L;

    public static final String STREAM_SPLIT_ID = "stream-split";

    private final String database;

    private final String collection;

    private final MongoStreamOffset offset;

    public MongoStreamSourceSplit(
            String splitId, String database, String collection, MongoStreamOffset offset) {
        super(splitId);
        this.database = database;
        this.collection = collection;
        this.offset = offset;
    }

    public String getDatabase() {
        return database;
    }

    public String getCollection() {
        return collection;
    }

    public MongoStreamOffset streamOffset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        MongoStreamSourceSplit split = (MongoStreamSourceSplit) o;
        return Objects.equals(database, split.database)
                && Objects.equals(collection, split.collection)
                && Objects.equals(offset, split.offset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), database, collection, offset);
    }

    @Override
    public String toString() {
        return "MongoStreamSourceSplit {"
                + " splitId="
                + splitId
                + ", database="
                + database
                + ", collection="
                + collection
                + ", offset="
                + offset
                + " }";
    }
}
