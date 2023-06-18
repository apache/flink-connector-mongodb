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

package org.apache.flink.connector.mongodb.source;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.mongodb.source.config.MongoStartupOptions;
import org.apache.flink.connector.mongodb.table.serialization.MongoRowDataDeserializationSchema;
import org.apache.flink.connector.mongodb.testutils.MongoTestUtil;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.RowUtils;
import org.apache.flink.util.CloseableIterator;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ChangeStreamPreAndPostImagesOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_FIELD;
import static org.apache.flink.connector.mongodb.testutils.MongoTestUtil.MONGO_4_0;
import static org.apache.flink.connector.mongodb.testutils.MongoTestUtil.MONGO_5_0;
import static org.apache.flink.connector.mongodb.testutils.MongoTestUtil.MONGO_6_0;
import static org.apache.flink.connector.mongodb.testutils.MongoTestUtil.MONGO_IMAGE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for using Mongo Source unbounded stream reading. */
@ExtendWith(ParameterizedTestExtension.class)
public class MongoUnboundedSourceITCase {

    private static final Logger LOG = LoggerFactory.getLogger(MongoUnboundedSourceITCase.class);

    private static final int PARALLELISM = 2;

    private static final String TEST_DATABASE = "test_stream_source";
    private static final String TEST_COLLECTION = "test_stream_coll";

    private static final int TEST_RECORD_SIZE = 30000;
    private static final int TEST_RECORD_BATCH_SIZE = 10000;

    @Parameter public String mongoVersion;

    @Parameter(value = 1)
    public FullDocument fullDocument;

    @Parameter(value = 2)
    public FullDocumentBeforeChange fullDocumentBeforeChange;

    @Parameters(name = "mongoVersion={0} fullDocument={1} fullDocumentBeforeChange={2}")
    public static List<Object[]> parameters() {
        return Arrays.asList(
                new Object[] {MONGO_4_0, FullDocument.UPDATE_LOOKUP, FullDocumentBeforeChange.OFF},
                new Object[] {MONGO_5_0, FullDocument.UPDATE_LOOKUP, FullDocumentBeforeChange.OFF},
                new Object[] {MONGO_6_0, FullDocument.UPDATE_LOOKUP, FullDocumentBeforeChange.OFF},
                new Object[] {MONGO_6_0, FullDocument.REQUIRED, FullDocumentBeforeChange.REQUIRED});
    }

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .withHaLeadershipControl()
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .build());

    private MongoDBContainer mongoDBContainer;
    private MongoClient mongoClient;
    private MongoCollection<BsonDocument> testCollection;

    @BeforeEach
    void before() {
        mongoDBContainer =
                MongoTestUtil.createMongoDBContainer(MONGO_IMAGE_PREFIX + mongoVersion, LOG);
        mongoDBContainer.start();
        mongoClient = MongoClients.create(mongoDBContainer.getConnectionString());

        MongoDatabase testDatabase = mongoClient.getDatabase(TEST_DATABASE);
        if (MONGO_6_0.equals(mongoVersion)) {
            testDatabase.createCollection(
                    TEST_COLLECTION,
                    new CreateCollectionOptions()
                            .changeStreamPreAndPostImagesOptions(
                                    new ChangeStreamPreAndPostImagesOptions(true)));
        }
        testCollection =
                testDatabase.getCollection(TEST_COLLECTION).withDocumentClass(BsonDocument.class);
        initTestData();
    }

    @AfterEach
    void after() {
        mongoClient.close();
        mongoDBContainer.close();
    }

    @TestTemplate
    void testInitialStartup(@InjectMiniCluster MiniCluster miniCluster) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(300L);

        MongoSource<RowData> mongoSource = createSource(MongoStartupOptions.initial());

        DataStreamSource<RowData> source =
                env.fromSource(mongoSource, WatermarkStrategy.noWatermarks(), "MongoDB-Source");

        CloseableIterator<RowData> iterator = source.collectAsync();
        JobClient jobClient = env.executeAsync();

        // Trigger failover once some snapshot records has been sent by sleeping source
        if (iterator.hasNext()) {
            triggerFailover(
                    FailoverType.JM, jobClient.getJobID(), () -> sleepMs(1000), miniCluster);
        }

        List<String> initialRows = fetchRowData(iterator, TEST_RECORD_SIZE);
        // assert scanned rows
        assertThat(initialRows).hasSize(TEST_RECORD_SIZE);

        // assert changed rows
        generateAndAssertStreamChanges(miniCluster, jobClient, iterator);
    }

    @TestTemplate
    void testTimestampStartup(@InjectMiniCluster MiniCluster miniCluster) throws Exception {
        // Unfortunately we need this sleep here to make sure we could differ the coming oplog
        // events from existing events by timestamp.
        sleepMs(2000L);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(300L);

        MongoSource<RowData> mongoSource =
                createSource(MongoStartupOptions.timestamp(System.currentTimeMillis()));

        DataStreamSource<RowData> source =
                env.fromSource(mongoSource, WatermarkStrategy.noWatermarks(), "MongoDB-Source");

        CloseableIterator<RowData> iterator = source.collectAsync();
        JobClient jobClient = env.executeAsync();

        generateAndAssertStreamChanges(miniCluster, jobClient, iterator);
    }

    @TestTemplate
    void testLatestStartup(@InjectMiniCluster MiniCluster miniCluster) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(300L);

        MongoSource<RowData> mongoSource = createSource(MongoStartupOptions.latest());
        DataStreamSource<RowData> source =
                env.fromSource(mongoSource, WatermarkStrategy.noWatermarks(), "MongoDB-Source");

        CloseableIterator<RowData> iterator = source.collectAsync();
        JobClient jobClient = env.executeAsync();

        // Unfortunately we need this sleep here to make sure we could differ the coming oplog
        // events from existing events by timestamp.
        sleepMs(2000L);

        generateAndAssertStreamChanges(miniCluster, jobClient, iterator);
    }

    private void generateAndAssertStreamChanges(
            MiniCluster miniCluster, JobClient jobClient, CloseableIterator<RowData> iterator)
            throws Exception {
        // insert 3
        int index = TEST_RECORD_SIZE + 1;
        List<BsonDocument> inserts =
                Arrays.asList(
                        createTestData(index),
                        createTestData(index + 1),
                        createTestData(index + 2));
        testCollection.insertMany(inserts);

        // delete 1
        testCollection.deleteOne(Filters.eq(ID_FIELD, inserts.get(2).getInt32(ID_FIELD)));

        triggerFailover(FailoverType.TM, jobClient.getJobID(), () -> sleepMs(300L), miniCluster);

        // update 1
        testCollection.updateOne(
                Filters.eq(ID_FIELD, inserts.get(1).getInt32(ID_FIELD)),
                Updates.set("name", new BsonString("name_updated")));

        // replace 1
        Bson filter = Filters.eq(ID_FIELD, inserts.get(0).getInt32(ID_FIELD));
        BsonDocument replacement =
                new BsonDocument("name", new BsonString("name_replaced"))
                        .append("address", new BsonString("address_replaced"));
        testCollection.replaceOne(filter, replacement);

        if (fullDocumentBeforeChange == FullDocumentBeforeChange.REQUIRED) {
            // all changelog mode
            List<String> allRows = fetchRowData(iterator, 8);
            assertThat(allRows)
                    .containsExactly(
                            "+I[30001, name_30001, address_30001]",
                            "+I[30002, name_30002, address_30002]",
                            "+I[30003, name_30003, address_30003]",
                            "-D[30003, name_30003, address_30003]",
                            "-U[30002, name_30002, address_30002]",
                            "+U[30002, name_updated, address_30002]",
                            "-U[30001, name_30001, address_30001]",
                            "+U[30001, name_replaced, address_replaced]");
        } else {
            // upsert changelog mode
            List<String> upsertRows = fetchRowData(iterator, 6);
            assertThat(upsertRows)
                    .containsExactly(
                            "+I[30001, name_30001, address_30001]",
                            "+I[30002, name_30002, address_30002]",
                            "+I[30003, name_30003, address_30003]",
                            "-D[30003, , ]",
                            "+U[30002, name_updated, address_30002]",
                            "+U[30001, name_replaced, address_replaced]");
        }

        iterator.close();
    }

    private MongoSource<RowData> createSource(MongoStartupOptions startupOptions) {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("_id", DataTypes.INT()),
                        Column.physical("name", DataTypes.STRING()),
                        Column.physical("address", DataTypes.STRING()));

        RowType rowType = (RowType) schema.toPhysicalRowDataType().getLogicalType();
        TypeInformation<RowData> typeInfo = InternalTypeInfo.of(rowType);

        return MongoSource.<RowData>builder()
                .setUri(mongoDBContainer.getConnectionString())
                .setDatabase(TEST_DATABASE)
                .setCollection(TEST_COLLECTION)
                .setStartupOptions(startupOptions)
                .setPartitionSize(MemorySize.parse("1mb"))
                .setFullDocument(fullDocument)
                .setFullDocumentBeforeChange(fullDocumentBeforeChange)
                .setDeserializationSchema(new MongoRowDataDeserializationSchema(rowType, typeInfo))
                .build();
    }

    private void initTestData() {
        List<BsonDocument> testRecords = new ArrayList<>();
        for (int i = 1; i <= TEST_RECORD_SIZE; i++) {
            testRecords.add(createTestData(i));
            if (testRecords.size() >= TEST_RECORD_BATCH_SIZE) {
                testCollection.insertMany(testRecords);
                testRecords.clear();
            }
        }
    }

    private static BsonDocument createTestData(int id) {
        return new BsonDocument("_id", new BsonInt32(id))
                .append("name", new BsonString("name_" + id))
                .append("address", new BsonString("address_" + id));
    }

    private static List<String> fetchRowData(Iterator<RowData> iter, int size) {
        List<RowData> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            RowData row = iter.next();
            rows.add(row);
            size--;
        }
        return convertRowDataToRowString(rows);
    }

    private static List<String> convertRowDataToRowString(List<RowData> rows) {
        LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
        map.put("_id", 0);
        map.put("name", 1);
        map.put("address", 2);
        return rows.stream()
                .map(
                        row ->
                                RowUtils.createRowWithNamedPositions(
                                                row.getRowKind(),
                                                new Object[] {
                                                    row.getInt(0),
                                                    row.getString(1),
                                                    row.getString(2)
                                                },
                                                map)
                                        .toString())
                .collect(Collectors.toList());
    }

    private enum FailoverType {
        NONE,
        TM,
        JM
    }

    private static void triggerFailover(
            FailoverType type, JobID jobId, Runnable afterFailAction, MiniCluster miniCluster)
            throws Exception {
        switch (type) {
            case NONE:
                afterFailAction.run();
                break;
            case TM:
                restartTaskManager(afterFailAction, miniCluster);
                break;
            case JM:
                triggerJobManagerFailover(jobId, afterFailAction, miniCluster);
                break;
        }
    }

    private static void triggerJobManagerFailover(
            JobID jobId, Runnable afterFailAction, MiniCluster miniCluster) throws Exception {
        final HaLeadershipControl haLeadershipControl = miniCluster.getHaLeadershipControl().get();
        haLeadershipControl.revokeJobMasterLeadership(jobId).get();
        afterFailAction.run();
        haLeadershipControl.grantJobMasterLeadership(jobId).get();
    }

    private static void restartTaskManager(Runnable afterFailAction, MiniCluster miniCluster)
            throws Exception {
        miniCluster.terminateTaskManager(0).get();
        afterFailAction.run();
        miniCluster.startTaskManager();
    }

    private static void sleepMs(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }
}
