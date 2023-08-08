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
import org.apache.flink.connector.mongodb.common.config.MongoConnectionOptions;
import org.apache.flink.connector.mongodb.table.converter.BsonToRowDataConverters;
import org.apache.flink.connector.mongodb.table.converter.RowDataToBsonConverters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static org.apache.flink.connector.mongodb.common.utils.MongoUtils.clientFor;
import static org.apache.flink.connector.mongodb.common.utils.MongoUtils.project;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A lookup function for {@link MongoDynamicTableSource}. */
@Internal
public class MongoRowDataLookupFunction extends LookupFunction {

    private static final Logger LOG = LoggerFactory.getLogger(MongoRowDataLookupFunction.class);
    private static final long serialVersionUID = 1L;

    private final MongoConnectionOptions connectionOptions;
    private final int maxRetries;
    private final long retryIntervalMs;

    private final List<String> fieldNames;
    private final List<String> keyNames;

    private final BsonToRowDataConverters.BsonToRowDataConverter mongoRowConverter;
    private final RowDataToBsonConverters.RowDataToBsonConverter lookupKeyRowConverter;

    private transient MongoClient mongoClient;

    public MongoRowDataLookupFunction(
            MongoConnectionOptions connectionOptions,
            int maxRetries,
            long retryIntervalMs,
            List<String> fieldNames,
            List<DataType> fieldTypes,
            List<String> keyNames,
            RowType rowType) {
        checkNotNull(fieldNames, "No fieldNames supplied.");
        checkNotNull(fieldTypes, "No fieldTypes supplied.");
        checkNotNull(keyNames, "No keyNames supplied.");
        this.connectionOptions = checkNotNull(connectionOptions);
        this.maxRetries = maxRetries;
        this.retryIntervalMs = retryIntervalMs;
        this.fieldNames = fieldNames;
        this.mongoRowConverter = BsonToRowDataConverters.createConverter(rowType);

        this.keyNames = keyNames;
        LogicalType[] keyTypes =
                this.keyNames.stream()
                        .map(s -> fieldTypes.get(fieldNames.indexOf(s)).getLogicalType())
                        .toArray(LogicalType[]::new);

        this.lookupKeyRowConverter =
                RowDataToBsonConverters.createConverter(
                        RowType.of(keyTypes, keyNames.toArray(new String[0])));
    }

    @Override
    public void open(FunctionContext context) {
        this.mongoClient = clientFor(connectionOptions);
    }

    /**
     * This is a lookup method which is called by Flink framework in runtime.
     *
     * @param keyRow lookup keys
     */
    @Override
    public Collection<RowData> lookup(RowData keyRow) {
        for (int retry = 0; retry <= maxRetries; retry++) {
            try {
                BsonDocument lookupValues = lookupKeyRowConverter.convert(keyRow);

                List<Bson> filters =
                        keyNames.stream()
                                .map(name -> eq(name, lookupValues.get(name)))
                                .collect(Collectors.toList());
                Bson query = and(filters);

                Bson projection = project(fieldNames);

                try (MongoCursor<BsonDocument> cursor =
                        getMongoCollection().find(query).projection(projection).cursor()) {
                    List<RowData> rows = new ArrayList<>();
                    while (cursor.hasNext()) {
                        RowData row = mongoRowConverter.convert(cursor.next());
                        rows.add(row);
                    }
                    return rows;
                }
            } catch (MongoException e) {
                LOG.debug("MongoDB lookup error, retry times = {}", retry, e);
                if (retry == maxRetries) {
                    LOG.error("MongoDB lookup error", e);
                    throw new RuntimeException("Execution of MongoDB lookup failed.", e);
                }
                try {
                    Thread.sleep(retryIntervalMs * (retry + 1));
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e1);
                }
            }
        }
        return Collections.emptyList();
    }

    private MongoCollection<BsonDocument> getMongoCollection() {
        return mongoClient
                .getDatabase(connectionOptions.getDatabase())
                .getCollection(connectionOptions.getCollection())
                .withDocumentClass(BsonDocument.class);
    }

    @Override
    public void close() throws IOException {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
