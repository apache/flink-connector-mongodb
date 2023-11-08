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
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.resolver.ExpressionResolver;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.expressions.RexNodeExpression;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.plan.utils.RexNodeToExpressionConverter;
import org.apache.flink.table.types.logical.RowType;

import com.mongodb.client.model.Filters;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.stream.Collectors;

import scala.Option;

import static org.apache.flink.connector.mongodb.table.MongoDynamicTableSource.parseFilter;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MongoFilterPushDownVisitor}. */
class MongoFilterPushDownVisitorTest {

    private static final String INPUT_TABLE = "mongo_source";

    private static final BsonDocument EMPTY_FILTER = Filters.empty().toBsonDocument();

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
                        + "boolean_col BOOLEAN,"
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
                            "boolean_col = true", Filters.eq("boolean_col", new BsonBoolean(true))
                        },
                        new Object[] {
                            "boolean_col = false", Filters.eq("boolean_col", new BsonBoolean(false))
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
        assertGeneratedFilter(
                "id = 6 AND timestamp_col = TIMESTAMP '2022-01-01 07:00:01'",
                schema,
                Filters.and(
                                Filters.eq("id", new BsonInt32(6)),
                                Filters.eq(
                                        "timestamp_col",
                                        new BsonDateTime(
                                                Timestamp.valueOf("2022-01-01 07:00:01")
                                                        .getTime())))
                        .toBsonDocument());

        assertGeneratedFilter(
                "timestamp3_col = TIMESTAMP '2022-01-01 07:00:01.333' OR description = 'Halo'",
                schema,
                Filters.or(
                                Filters.eq(
                                        "timestamp3_col",
                                        new BsonDateTime(
                                                Timestamp.valueOf("2022-01-01 07:00:01.333")
                                                        .getTime())),
                                Filters.eq("description", new BsonString("Halo")))
                        .toBsonDocument());
    }

    @Test
    void testExpressionWithNull() {
        ResolvedSchema schema = tEnv.sqlQuery("SELECT * FROM " + INPUT_TABLE).getResolvedSchema();

        assertGeneratedFilter(
                "id = NULL AND decimal_col <= 0.6",
                schema,
                Filters.and(
                                Filters.eq("id", BsonNull.VALUE),
                                Filters.lte(
                                        "decimal_col",
                                        new BsonDecimal128(new Decimal128(new BigDecimal("0.6")))))
                        .toBsonDocument());

        assertGeneratedFilter(
                "id = 6 OR description = NULL",
                schema,
                Filters.or(
                                Filters.eq("id", new BsonInt32(6)),
                                Filters.eq("description", BsonNull.VALUE))
                        .toBsonDocument());
    }

    @Test
    void testExpressionIsNull() {
        ResolvedSchema schema = tEnv.sqlQuery("SELECT * FROM " + INPUT_TABLE).getResolvedSchema();

        assertGeneratedFilter(
                "id IS NULL AND decimal_col <= 0.6",
                schema,
                Filters.and(
                                Filters.eq("id", BsonNull.VALUE),
                                Filters.lte(
                                        "decimal_col",
                                        new BsonDecimal128(new Decimal128(new BigDecimal("0.6")))))
                        .toBsonDocument());

        assertGeneratedFilter(
                "id = 6 OR description IS NOT NULL",
                schema,
                Filters.or(
                                Filters.eq("id", new BsonInt32(6)),
                                Filters.ne("description", BsonNull.VALUE))
                        .toBsonDocument());
    }

    @Test
    void testExpressionCannotBePushedDown() {
        ResolvedSchema schema = tEnv.sqlQuery("SELECT * FROM " + INPUT_TABLE).getResolvedSchema();

        // unsupported operators
        assertGeneratedFilter("description LIKE '_bcd%'", schema, EMPTY_FILTER);

        // nested complex expressions
        assertGeneratedFilter("double_col = decimal_col", schema, EMPTY_FILTER);
        assertGeneratedFilter("boolean_col = (decimal_col > 2.0)", schema, EMPTY_FILTER);

        // partial push down
        assertGeneratedFilter(
                "id IS NULL AND description LIKE '_bcd%'",
                schema, Filters.eq("id", BsonNull.VALUE).toBsonDocument());

        // sub filter cannot be pushed down
        assertGeneratedFilter("id IS NOT NULL OR double_col = decimal_col", schema, EMPTY_FILTER);
    }

    private void assertGeneratedFilter(
            String inputExpr, ResolvedSchema schema, BsonDocument expected) {
        List<ResolvedExpression> filters = resolveSQLFilterToExpression(inputExpr, schema);

        List<Bson> mongoFilters = new ArrayList<>();
        for (ResolvedExpression filter : filters) {
            BsonDocument simpleFilter = parseFilter(filter);
            if (!simpleFilter.isEmpty()) {
                mongoFilters.add(simpleFilter);
            }
        }

        BsonDocument actual = EMPTY_FILTER;
        if (!mongoFilters.isEmpty()) {
            Bson mergedFilter =
                    mongoFilters.size() == 1 ? mongoFilters.get(0) : Filters.and(mongoFilters);
            actual = mergedFilter.toBsonDocument();
        }

        assertThat(actual).isEqualTo(expected);
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
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RexNodeToExpressionConverter converter =
                new RexNodeToExpressionConverter(
                        rexBuilder,
                        sourceType.getFieldNames().toArray(new String[0]),
                        funCat,
                        catMan,
                        TimeZone.getTimeZone(tEnv.getConfig().getLocalTimeZone()));

        RexNodeExpression rexExp =
                (RexNodeExpression) tbImpl.getParser().parseSqlExpression(sqlExp, sourceType, null);

        RexNode cnf = FlinkRexUtil.toCnf(rexBuilder, -1, rexExp.getRexNode());
        // converts the cnf condition to a list of AND conditions
        List<RexNode> conjunctions = RelOptUtil.conjunctions(cnf);

        List<Expression> resolvedExps =
                conjunctions.stream()
                        .map(rex -> rex.accept(converter))
                        .filter(Option::isDefined)
                        .map(Option::get)
                        .collect(Collectors.toList());

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

        return resolver.resolve(resolvedExps);
    }
}
