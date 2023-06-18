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

package org.apache.flink.connector.mongodb.table.serialization;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.mongodb.source.reader.MongoSourceRecord;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.connector.mongodb.table.converter.BsonToRowDataConverters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import com.mongodb.client.model.changestream.OperationType;
import org.bson.BsonDocument;

import java.util.NoSuchElementException;
import java.util.Optional;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.DOCUMENT_KEY_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.FULL_DOCUMENT_BEFORE_CHANGE_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.FULL_DOCUMENT_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.OPERATION_TYPE_FIELD;

/** Deserializer that maps {@link BsonDocument} to {@link RowData}. */
@Internal
public class MongoRowDataDeserializationSchema implements MongoDeserializationSchema<RowData> {

    /** Type information describing the result type. */
    private final TypeInformation<RowData> typeInfo;

    /** Runtime instance that performs the actual work. */
    private final BsonToRowDataConverters.BsonToRowDataConverter runtimeConverter;

    public MongoRowDataDeserializationSchema(RowType rowType, TypeInformation<RowData> typeInfo) {
        this.typeInfo = typeInfo;
        this.runtimeConverter = BsonToRowDataConverters.createConverter(rowType);
    }

    @Override
    public void deserialize(MongoSourceRecord sourceRecord, Collector<RowData> out) {
        switch (sourceRecord.getType()) {
            case SNAPSHOT:
                out.collect(runtimeConverter.convert(sourceRecord.getRecord()));
                break;
            case STREAM:
                deserializeStreamRecord(sourceRecord, out);
                break;
            default:
                throw new IllegalStateException(
                        "Unsupported record type " + sourceRecord.getType());
        }
    }

    private void deserializeStreamRecord(MongoSourceRecord sourceRecord, Collector<RowData> out) {
        BsonDocument changeStreamDocument = sourceRecord.getRecord();
        OperationType operationType = getOperationType(changeStreamDocument);
        switch (operationType) {
            case INSERT:
                {
                    BsonDocument fullDocument =
                            getFullDocument(changeStreamDocument)
                                    .orElseThrow(NoSuchElementException::new);
                    RowData insert = runtimeConverter.convert(fullDocument);
                    insert.setRowKind(RowKind.INSERT);
                    out.collect(insert);
                    break;
                }
            case UPDATE:
            case REPLACE:
                {
                    Optional<BsonDocument> updateBefore =
                            getFullDocumentBeforeChange(changeStreamDocument);
                    updateBefore.ifPresent(
                            doc -> {
                                RowData before = runtimeConverter.convert(doc);
                                before.setRowKind(RowKind.UPDATE_BEFORE);
                                out.collect(before);
                            });

                    Optional<BsonDocument> fullDocument = getFullDocument(changeStreamDocument);
                    fullDocument.ifPresent(
                            doc -> {
                                RowData after = runtimeConverter.convert(doc);
                                after.setRowKind(RowKind.UPDATE_AFTER);
                                out.collect(after);
                            });
                    break;
                }
            case DELETE:
                {
                    BsonDocument document =
                            getFullDocumentBeforeChange(changeStreamDocument)
                                    .orElse(getDocumentKey(changeStreamDocument));
                    RowData delete = runtimeConverter.convert(document);
                    delete.setRowKind(RowKind.DELETE);
                    out.collect(delete);
                    break;
                }
            default:
                // ignore ddl and other record
                break;
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return typeInfo;
    }

    private static OperationType getOperationType(BsonDocument changeStreamDocument) {
        return OperationType.fromString(
                changeStreamDocument.getString(OPERATION_TYPE_FIELD).getValue());
    }

    private static BsonDocument getDocumentKey(BsonDocument changeStreamDocument) {
        return changeStreamDocument.getDocument(DOCUMENT_KEY_FIELD);
    }

    private static Optional<BsonDocument> getFullDocument(BsonDocument changeStreamDocument) {
        if (changeStreamDocument.containsKey(FULL_DOCUMENT_FIELD)) {
            return Optional.of(changeStreamDocument.getDocument(FULL_DOCUMENT_FIELD));
        }
        return Optional.empty();
    }

    private static Optional<BsonDocument> getFullDocumentBeforeChange(
            BsonDocument changeStreamDocument) {
        if (changeStreamDocument.containsKey(FULL_DOCUMENT_BEFORE_CHANGE_FIELD)) {
            return Optional.of(changeStreamDocument.getDocument(FULL_DOCUMENT_BEFORE_CHANGE_FIELD));
        }
        return Optional.empty();
    }
}
