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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.resolver.ExpressionResolver;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.expressions.RexNodeExpression;
import org.apache.flink.table.planner.plan.utils.RexNodeToExpressionConverter;
import org.apache.flink.table.types.logical.RowType;

import com.mongodb.client.model.Filters;
import org.apache.calcite.rex.RexBuilder;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MongoFilterPushDownVisitor}. */
class MongoFilterPushDownVisitorTest {

    public static final String INPUT_TABLE = "mongo_source";

    private static StreamExecutionEnvironment env;
    private static StreamTableEnvironment tEnv;

    private final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    @BeforeEach
    void before() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));

        // Create table in Flink, this can be reused across test cases
        tEnv.executeSql(
                "CREATE TABLE "
                        + INPUT_TABLE
                        + "("
                        + "id INTEGER,"
                        + "description VARCHAR(200),"
                        + "timestamp_col TIMESTAMP(0),"
                        + "timestamp3_col TIMESTAMP(3),"
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
    void testSimpleExpressionPrimitiveType() {
        ResolvedSchema schema = tEnv.sqlQuery("SELECT * FROM " + INPUT_TABLE).getResolvedSchema();
        Arrays.asList(
                        new Object[] {"id = 6", Filters.eq("id", new BsonInt32(6))},
                        new Object[] {"id >= 6", Filters.gte("id", new BsonInt32(6))},
                        new Object[] {"id > 6", Filters.gt("id", new BsonInt32(6))},
                        new Object[] {"id < 6", Filters.lt("id", new BsonInt32(6))},
                        new Object[] {"id <= 5", Filters.lte("id", 5)},
                        new Object[] {
                            "description = 'Halo'",
                            Filters.eq("description", new BsonString("Halo"))
                        },
                        new Object[] {
                            "double_col > 0.5",
                            Filters.gt(
                                    "double_col",
                                    new BsonDecimal128(new Decimal128(new BigDecimal("0.5"))))
                        },
                        new Object[] {
                            "decimal_col <= -0.3",
                            Filters.lte(
                                    "decimal_col",
                                    new BsonDecimal128(new Decimal128(new BigDecimal("-0.3"))))
                        })
                .forEach(
                        inputs ->
                                assertGeneratedFilter(
                                        (String) inputs[0],
                                        schema,
                                        ((Bson) inputs[1]).toBsonDocument()));
    }

    @Test
    void testComplexExpressionDatetime() {
        ResolvedSchema schema = tEnv.sqlQuery("SELECT * FROM " + INPUT_TABLE).getResolvedSchema();
        String andExpr = "id = 6 AND timestamp_col = TIMESTAMP '2022-01-01 07:00:01'";
        assertGeneratedFilter(
                andExpr,
                schema,
                Filters.and(
                                Filters.eq("id", new BsonInt32(6)),
                                Filters.eq("timestamp_col", new BsonDateTime(1640991601000L)))
                        .toBsonDocument());

        String orExpr =
                "timestamp3_col = TIMESTAMP '2022-01-01 07:00:01.333' OR description = 'Halo'";
        assertGeneratedFilter(
                orExpr,
                schema,
                Filters.or(
                                Filters.eq("timestamp3_col", new BsonDateTime(1640991601333L)),
                                Filters.eq("description", new BsonString("Halo")))
                        .toBsonDocument());
    }

    @Test
    void testExpressionWithNull() {
        ResolvedSchema schema = tEnv.sqlQuery("SELECT * FROM " + INPUT_TABLE).getResolvedSchema();
        String andExpr = "id = NULL AND decimal_col <= 0.6";

        assertGeneratedFilter(
                andExpr,
                schema,
                Filters.and(
                                Filters.eq("id", BsonNull.VALUE),
                                Filters.lte(
                                        "decimal_col",
                                        new BsonDecimal128(new Decimal128(new BigDecimal("0.6")))))
                        .toBsonDocument());

        String orExpr = "id = 6 OR description = NULL";
        assertGeneratedFilter(
                orExpr,
                schema,
                Filters.or(
                                Filters.eq("id", new BsonInt32(6)),
                                Filters.eq("description", BsonNull.VALUE))
                        .toBsonDocument());
    }

    @Test
    void testExpressionIsNull() {
        ResolvedSchema schema = tEnv.sqlQuery("SELECT * FROM " + INPUT_TABLE).getResolvedSchema();
        String andExpr = "id IS NULL AND decimal_col <= 0.6";

        assertGeneratedFilter(
                andExpr,
                schema,
                Filters.and(
                                Filters.eq("id", BsonNull.VALUE),
                                Filters.lte(
                                        "decimal_col",
                                        new BsonDecimal128(new Decimal128(new BigDecimal("0.6")))))
                        .toBsonDocument());

        String orExpr = "id = 6 OR description IS NOT NULL";
        assertGeneratedFilter(
                orExpr,
                schema,
                Filters.or(
                                Filters.eq("id", new BsonInt32(6)),
                                Filters.ne("description", BsonNull.VALUE))
                        .toBsonDocument());
    }

    private void assertGeneratedFilter(
            String inputExpr, ResolvedSchema schema, BsonDocument expectedFilter) {
        List<ResolvedExpression> resolved = resolveSQLFilterToExpression(inputExpr, schema);
        assertThat(resolved.size()).isEqualTo(1);
        BsonValue filter = resolved.get(0).accept(MongoFilterPushDownVisitor.INSTANCE);
        assertThat(filter).isEqualTo(expectedFilter);
    }

    /**
     * Resolve a SQL filter expression against a Schema, this method makes use of some
     * implementation details of Flink.
     */
    private List<ResolvedExpression> resolveSQLFilterToExpression(
            String sqlExp, ResolvedSchema schema) {
        StreamTableEnvironmentImpl tbImpl = (StreamTableEnvironmentImpl) tEnv;

        FlinkContext ctx = ((PlannerBase) tbImpl.getPlanner()).getFlinkContext();
        CatalogManager catMan = tbImpl.getCatalogManager();
        FunctionCatalog funCat = ctx.getFunctionCatalog();
        RowType sourceType = (RowType) schema.toSourceRowDataType().getLogicalType();

        FlinkTypeFactory typeFactory = new FlinkTypeFactory(classLoader, FlinkTypeSystem.INSTANCE);
        RexNodeToExpressionConverter converter =
                new RexNodeToExpressionConverter(
                        new RexBuilder(typeFactory),
                        sourceType.getFieldNames().toArray(new String[0]),
                        funCat,
                        catMan,
                        TimeZone.getTimeZone(tEnv.getConfig().getLocalTimeZone()));

        RexNodeExpression rexExp =
                (RexNodeExpression) tbImpl.getParser().parseSqlExpression(sqlExp, sourceType, null);
        ResolvedExpression resolvedExp =
                rexExp.getRexNode()
                        .accept(converter)
                        .getOrElse(
                                () -> {
                                    throw new IllegalArgumentException(
                                            "Cannot convert "
                                                    + rexExp.getRexNode()
                                                    + " to Expression, this likely "
                                                    + "means you used some function(s) not "
                                                    + "supported with this setup.");
                                });
        ExpressionResolver resolver =
                ExpressionResolver.resolverFor(
                                tEnv.getConfig(),
                                classLoader,
                                name -> Optional.empty(),
                                funCat.asLookup(
                                        str -> {
                                            throw new TableException(
                                                    "We should not need to lookup any expressions at this point");
                                        }),
                                catMan.getDataTypeFactory(),
                                (sqlExpression, inputRowType, outputType) -> {
                                    throw new TableException(
                                            "SQL expression parsing is not supported at this location.");
                                })
                        .build();

        return resolver.resolve(Collections.singletonList(resolvedExp));
    }
}
