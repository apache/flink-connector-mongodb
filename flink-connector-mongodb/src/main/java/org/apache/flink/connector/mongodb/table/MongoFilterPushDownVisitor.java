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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionDefaultVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.logical.LogicalType;

import com.mongodb.client.model.Filters;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonUndefined;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Visitor that convert Expression to Bson filter. Return {@link Filters#empty()} if we cannot push
 * down the filter.
 */
@Experimental
public class MongoFilterPushDownVisitor extends ExpressionDefaultVisitor<BsonValue> {

    public static final MongoFilterPushDownVisitor INSTANCE = new MongoFilterPushDownVisitor();

    private MongoFilterPushDownVisitor() {}

    @Override
    public BsonDocument visit(CallExpression call) {
        Bson filter = Filters.empty();
        if (BuiltInFunctionDefinitions.EQUALS.equals(call.getFunctionDefinition())) {
            filter = renderBinaryComparisonOperator("$eq", call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.LESS_THAN.equals(call.getFunctionDefinition())) {
            filter = renderBinaryComparisonOperator("$lt", call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL.equals(call.getFunctionDefinition())) {
            filter = renderBinaryComparisonOperator("$lte", call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.GREATER_THAN.equals(call.getFunctionDefinition())) {
            filter = renderBinaryComparisonOperator("$gt", call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL.equals(call.getFunctionDefinition())) {
            filter = renderBinaryComparisonOperator("$gte", call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.NOT_EQUALS.equals(call.getFunctionDefinition())) {
            filter = renderBinaryComparisonOperator("$ne", call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.IS_NULL.equals(call.getFunctionDefinition())) {
            filter =
                    renderUnaryComparisonOperator(
                            "$eq", call.getResolvedChildren().get(0), BsonNull.VALUE);
        }
        if (BuiltInFunctionDefinitions.IS_NOT_NULL.equals(call.getFunctionDefinition())) {
            filter =
                    renderUnaryComparisonOperator(
                            "$ne", call.getResolvedChildren().get(0), BsonNull.VALUE);
        }
        if (BuiltInFunctionDefinitions.OR.equals(call.getFunctionDefinition())) {
            filter = renderLogicalOperator("$or", call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.AND.equals(call.getFunctionDefinition())) {
            filter = renderLogicalOperator("$and", call.getResolvedChildren());
        }
        return filter.toBsonDocument();
    }

    private Bson renderBinaryComparisonOperator(
            String operator, List<ResolvedExpression> expressions) {
        Optional<FieldReferenceExpression> fieldReferenceExpr =
                extractExpression(expressions, FieldReferenceExpression.class);
        Optional<ValueLiteralExpression> fieldValueExpr =
                extractExpression(expressions, ValueLiteralExpression.class);

        // Nested complex expressions are not supported. e.g. f1 = (f2 > 2)
        if (!fieldReferenceExpr.isPresent() || !fieldValueExpr.isPresent()) {
            return Filters.empty();
        }

        String fieldName = visit(fieldReferenceExpr.get()).getValue();
        BsonValue fieldValue = visit(fieldValueExpr.get());

        // Unsupported values
        if (fieldValue.getBsonType() == BsonType.UNDEFINED) {
            return Filters.empty();
        }

        switch (operator) {
            case "$eq":
                return Filters.eq(fieldName, fieldValue);
            case "$lt":
                return Filters.lt(fieldName, fieldValue);
            case "$lte":
                return Filters.lte(fieldName, fieldValue);
            case "$gt":
                return Filters.gt(fieldName, fieldValue);
            case "$gte":
                return Filters.gte(fieldName, fieldValue);
            case "$ne":
                return Filters.ne(fieldName, fieldValue);
            default:
                return Filters.empty();
        }
    }

    private Bson renderUnaryComparisonOperator(
            String operator, ResolvedExpression operand, BsonValue value) {
        if (operand instanceof FieldReferenceExpression) {
            String fieldName = visit((FieldReferenceExpression) operand).getValue();
            switch (operator) {
                case "$eq":
                    return Filters.eq(fieldName, value);
                case "$ne":
                    return Filters.ne(fieldName, value);
                default:
                    return Filters.empty();
            }
        } else {
            return Filters.empty();
        }
    }

    private Bson renderLogicalOperator(String operator, List<ResolvedExpression> operands) {
        Bson[] filters = new Bson[operands.size()];
        for (int i = 0; i < operands.size(); i++) {
            ResolvedExpression operand = operands.get(i);
            BsonValue filter = operand.accept(this);

            // sub-filters that cannot be pushed down
            if (!filter.isDocument() || filter.asDocument().isEmpty()) {
                return Filters.empty();
            }
            filters[i] = filter.asDocument();
        }

        switch (operator) {
            case "$or":
                return Filters.or(filters);
            case "$and":
                return Filters.and(filters);
            default:
                return Filters.empty();
        }
    }

    @Override
    public BsonValue visit(ValueLiteralExpression litExp) {
        LogicalType type = litExp.getOutputDataType().getLogicalType();
        Optional<BsonValue> value;
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                value = litExp.getValueAs(String.class).map(BsonString::new);
                break;
            case BOOLEAN:
                value = litExp.getValueAs(Boolean.class).map(BsonBoolean::new);
                break;
            case DECIMAL:
                value =
                        litExp.getValueAs(BigDecimal.class)
                                .map(Decimal128::new)
                                .map(BsonDecimal128::new);
                break;
            case INTEGER:
                value = litExp.getValueAs(Integer.class).map(BsonInt32::new);
                break;
            case BIGINT:
                value = litExp.getValueAs(Long.class).map(BsonInt64::new);
                break;
            case DOUBLE:
                value = litExp.getValueAs(Double.class).map(BsonDouble::new);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                value =
                        litExp.getValueAs(LocalDateTime.class)
                                .map(Timestamp::valueOf)
                                .map(Timestamp::getTime)
                                .map(BsonDateTime::new);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                value =
                        litExp.getValueAs(Instant.class)
                                .map(Instant::toEpochMilli)
                                .map(BsonDateTime::new);
                break;
            default:
                // Use BsonUndefined to represent unsupported values.
                value = Optional.of(new BsonUndefined());
                break;
        }
        return value.orElse(BsonNull.VALUE);
    }

    @Override
    public BsonString visit(FieldReferenceExpression fieldReference) {
        return new BsonString(fieldReference.toString());
    }

    @Override
    protected BsonDocument defaultMethod(Expression expression) {
        return Filters.empty().toBsonDocument();
    }

    private static <T> Optional<T> extractExpression(
            List<ResolvedExpression> expressions, Class<T> type) {
        for (ResolvedExpression expression : expressions) {
            if (type.isAssignableFrom(expression.getClass())) {
                return Optional.of(type.cast(expression));
            }
        }
        return Optional.empty();
    }
}
