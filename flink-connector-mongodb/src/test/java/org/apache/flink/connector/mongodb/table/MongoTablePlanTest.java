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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;

/** Plan tests for Mongo connector, for example, testing projection push down. */
public class MongoTablePlanTest extends TableTestBase {
    // TODO: Update to junit5 after TableTestBase migrated
    private final StreamTableTestUtil util = streamTestUtil(TableConfig.getDefault());

    @Before
    public void setup() {
        TableEnvironment tEnv = util.tableEnv();
        tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
        tEnv.executeSql(
                "CREATE TABLE mongo ("
                        + "id BIGINT,"
                        + "description VARCHAR(200),"
                        + "boolean_col BOOLEAN,"
                        + "timestamp_col TIMESTAMP_LTZ(0),"
                        + "timestamp3_col TIMESTAMP_LTZ(3),"
                        + "int_col INTEGER,"
                        + "double_col DOUBLE,"
                        + "decimal_col DECIMAL(10, 4)"
                        + ") WITH ("
                        + "  'connector'='mongodb',"
                        + "  'uri'='mongodb://127.0.0.1:27017',"
                        + "  'database'='test_db',"
                        + "  'collection'='test_coll'"
                        + ")");
    }

    @Test
    public void testFilterPushdown() {
        util.verifyExecPlan(
                "SELECT id, timestamp3_col, int_col FROM mongo WHERE id = 900001 AND timestamp3_col <> TIMESTAMP '2022-09-07 10:25:28.127' OR double_col >= -1000.23");
    }

    @Test
    public void testFilterPartialPushdown() {
        util.verifyExecPlan(
                "SELECT id, timestamp3_col, int_col FROM mongo WHERE id = 900001 AND boolean_col = (decimal_col > 2.0)");
    }

    @Test
    public void testFilterCannotPushdown() {
        util.verifyExecPlan(
                "SELECT id, timestamp3_col, int_col FROM mongo WHERE id IS NOT NULL OR double_col = decimal_col");
    }
}
