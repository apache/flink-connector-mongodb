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

import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.CHANGE_STREAM_FETCH_SIZE;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The change stream configuration class for MongoDB source. */
@PublicEvolving
public class MongoChangeStreamOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int fetchSize;

    private final FullDocument fullDocument;

    private final FullDocumentBeforeChange fullDocumentBeforeChange;

    private MongoChangeStreamOptions(
            int fetchSize,
            FullDocument fullDocument,
            FullDocumentBeforeChange fullDocumentBeforeChange) {
        this.fetchSize = fetchSize;
        this.fullDocument = fullDocument;
        this.fullDocumentBeforeChange = fullDocumentBeforeChange;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public FullDocument getFullDocument() {
        return fullDocument;
    }

    public FullDocumentBeforeChange getFullDocumentBeforeChange() {
        return fullDocumentBeforeChange;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoChangeStreamOptions that = (MongoChangeStreamOptions) o;
        return fetchSize == that.fetchSize
                && Objects.equals(fullDocument, that.fullDocument)
                && Objects.equals(fullDocumentBeforeChange, that.fullDocumentBeforeChange);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fetchSize, fullDocument, fullDocumentBeforeChange);
    }

    public static MongoChangeStreamOptionsBuilder builder() {
        return new MongoChangeStreamOptionsBuilder();
    }

    /** Builder for {@link MongoReadOptions}. */
    @PublicEvolving
    public static class MongoChangeStreamOptionsBuilder {
        private int fetchSize = CHANGE_STREAM_FETCH_SIZE.defaultValue();
        private FullDocument fullDocument = FullDocument.UPDATE_LOOKUP;
        private FullDocumentBeforeChange fullDocumentBeforeChange = FullDocumentBeforeChange.OFF;

        private MongoChangeStreamOptionsBuilder() {}

        /**
         * Sets the number of change stream documents should be fetched per round-trip when reading.
         *
         * @param fetchSize the number of change documents stream should be fetched per round-trip
         *     when reading.
         * @return this builder
         */
        public MongoChangeStreamOptionsBuilder setFetchSize(int fetchSize) {
            checkArgument(fetchSize > 0, "The change stream fetch size must be larger than 0.");
            this.fetchSize = fetchSize;
            return this;
        }

        /**
         * Determines what values your change stream returns on update operations. The default
         * setting returns the differences between the original document and the updated document.
         * The updateLookup setting returns the differences between the original document and
         * updated document as well as a copy of the entire updated document at a point in time
         * after the update. The whenAvailable setting returns the updated document, if available.
         * The required setting returns the updated document and raises an error if it is not
         * available.
         *
         * @param fullDocument the values your change stream returns on update operations.
         * @return this builder
         */
        public MongoChangeStreamOptionsBuilder setFullDocument(FullDocument fullDocument) {
            this.fullDocument = checkNotNull(fullDocument, "The fullDocument must not be null.");
            return this;
        }

        /**
         * Configures the document pre-image your change stream returns on update operations. The
         * pre-image is not available for source records published while copying existing data, and
         * the pre-image configuration has no effect on copying. The default setting suppresses the
         * document pre-image. The whenAvailable setting returns the document pre-image if it's
         * available, before it was replaced, updated, or deleted. The required setting returns the
         * document pre-image and raises an error if it is not available.
         *
         * @param fullDocumentBeforeChange the document pre-image your change stream returns on
         *     update operations.
         * @return this builder
         */
        public MongoChangeStreamOptionsBuilder setFullDocumentBeforeChange(
                FullDocumentBeforeChange fullDocumentBeforeChange) {
            this.fullDocumentBeforeChange =
                    checkNotNull(
                            fullDocumentBeforeChange,
                            "The fullDocumentBeforeChange must not be null.");
            return this;
        }

        /**
         * Build the {@link MongoReadOptions}.
         *
         * @return a MongoReadOptions with the settings made for this builder.
         */
        public MongoChangeStreamOptions build() {
            return new MongoChangeStreamOptions(fetchSize, fullDocument, fullDocumentBeforeChange);
        }
    }
}
