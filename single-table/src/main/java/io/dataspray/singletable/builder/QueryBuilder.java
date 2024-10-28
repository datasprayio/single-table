package io.dataspray.singletable.builder;

import io.dataspray.singletable.ExpressionBuilder;
import io.dataspray.singletable.Schema;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class QueryBuilder<T> extends ExpressionBuilder<T, QueryBuilder<T>> implements FilterExpressionBuilder<QueryBuilder<T>>, ConditionExpressionBuilder<QueryBuilder<T>> {

    public QueryBuilder(Schema<T> schema) {
        super(schema);
    }

    protected QueryBuilder<T> getParent() {
        return this;
    }

    private Optional<Map<String, Condition>> keyConditionsOpt = Optional.empty();

    public QueryBuilder<T> keyConditions(Map<String, Object> primaryKey) {
        checkState(!built);
        this.keyConditionsOpt = Optional.of(schema.attrMapToConditions(schema.primaryKey(primaryKey)));
        return this;
    }

    public QueryRequest.Builder builder() {
        Expression expression = buildExpression();
        QueryRequest.Builder builder = QueryRequest.builder();
        builder.tableName(schema.tableName());
        schema.indexNameOpt().ifPresent(builder::indexName);
        checkState(expression.updateExpression().isEmpty(), "Query does not support update expression");
        expression.conditionExpression().ifPresent(builder::filterExpression);
        expression.expressionAttributeNames().ifPresent(builder::expressionAttributeNames);
        expression.expressionAttributeValues().ifPresent(builder::expressionAttributeValues);
        keyConditionsOpt.ifPresent(builder::keyConditions);
        return builder;
    }

    public QueryRequest build() {
        return builder().build();
    }

    public QueryResponse execute(DynamoDbClient dynamo) {
        return dynamo.query(build());
    }
}
