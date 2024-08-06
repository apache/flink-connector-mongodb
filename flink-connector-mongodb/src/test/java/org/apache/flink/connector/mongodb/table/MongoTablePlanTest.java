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

package org.apache.flink.connector.mongodb.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.rules.TestName;

import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;

/** Plan tests for Mongo connector, for example, testing projection push down. */
class MongoTablePlanTest extends TableTestBase {

    private final StreamTableTestUtil util = streamTestUtil(TableConfig.getDefault());

    private TestInfo testInfo;

    @BeforeEach
    void setup(TestInfo testInfo) {
        this.testInfo = testInfo;
        TableEnvironment tEnv = util.tableEnv();
        tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    }

    @Test
    void testFilterPushdown() {
        createTestTable();
        util.verifyExecPlan(
                "SELECT id, timestamp3_col, int_col FROM mongo WHERE id = 900001 AND timestamp3_col <> TIMESTAMP '2022-09-07 10:25:28.127' OR double_col >= -1000.23");
    }

    @Test
    void testFilterPartialPushdown() {
        createTestTable();
        util.verifyExecPlan(
                "SELECT id, timestamp3_col, int_col FROM mongo WHERE id = 900001 AND boolean_col = (decimal_col > 2.0)");
    }

    @Test
    void testFilterCannotPushdown() {
        createTestTable();
        util.verifyExecPlan(
                "SELECT id, timestamp3_col, int_col FROM mongo WHERE id IS NOT NULL OR double_col = decimal_col");
    }

    @Test
    void testNeverFilterPushdown() {
        createTestTable(
                Collections.singletonMap(
                        MongoConnectorOptions.FILTER_HANDLING_POLICY.key(),
                        FilterHandlingPolicy.NEVER.name()));
        util.verifyExecPlan(
                "SELECT id, timestamp3_col, int_col FROM mongo WHERE id = 900001 AND decimal_col > 1.0");
    }

    private void createTestTable() {
        createTestTable(Collections.emptyMap());
    }

    private void createTestTable(Map<String, String> extraOptions) {
        TableDescriptor.Builder builder =
                TableDescriptor.forConnector("mongodb")
                        .option("uri", "mongodb://127.0.0.1:27017")
                        .option("database", "test_db")
                        .option("collection", "test_coll")
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.BIGINT())
                                        .column("description", DataTypes.VARCHAR(200))
                                        .column("boolean_col", DataTypes.BOOLEAN())
                                        .column("timestamp_col", DataTypes.TIMESTAMP_LTZ(0))
                                        .column("timestamp3_col", DataTypes.TIMESTAMP_LTZ(3))
                                        .column("int_col", DataTypes.INT())
                                        .column("double_col", DataTypes.DOUBLE())
                                        .column("decimal_col", DataTypes.DECIMAL(10, 4))
                                        .build());

        extraOptions.forEach(builder::option);

        util.tableEnv().createTable("mongo", builder.build());
    }

    // A workaround to get the test method name for flink versions not completely migrated to JUnit5
    public TestName name() {
        return new TestName() {
            @Override
            public String getMethodName() {
                return testInfo.getTestMethod().get().getName();
            }
        };
    }
}
