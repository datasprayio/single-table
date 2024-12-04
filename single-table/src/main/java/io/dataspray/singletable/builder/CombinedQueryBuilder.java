package io.dataspray.singletable.builder;

import com.google.common.collect.ImmutableSet;
import io.dataspray.singletable.ExpressionBuilder;
import io.dataspray.singletable.Schema;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;

public abstract class CombinedQueryBuilder<T, P> extends ExpressionBuilder<T, P, QueryRequest.Builder> implements FilterExpressionBuilder<P>, ConditionExpressionBuilder<P> {

    protected final Optional<String> indexNameOpt;

    public CombinedQueryBuilder(Schema<T> schema, Optional<String> indexNameOpt) {
        super(schema);
        this.indexNameOpt = indexNameOpt;
    }

    public QueryRequest.Builder builder() {
        Expression<QueryRequest.Builder> expression = buildExpression();
        QueryRequest.Builder builder = QueryRequest.builder();
        builder.tableName(schema.tableName());
        indexNameOpt.ifPresent(builder::indexName);
        checkState(expression.updateExpression().isEmpty(), "Query does not support update expression");
        expression.filterExpression().ifPresent(builder::filterExpression);
        expression.expressionAttributeNames().ifPresent(builder::expressionAttributeNames);
        expression.expressionAttributeValues().ifPresent(builder::expressionAttributeValues);
        expression.builderAdjustments().forEach(c -> c.accept(builder));
        return builder;
    }

    public QueryRequest build() {
        return builder().build();
    }

    public QueryResponse execute(DynamoDbClient dynamo) {
        return dynamo.query(build());
    }

    public Stream<T> executeStream(DynamoDbClient dynamo) {
        return executeStreamBatch(dynamo).flatMap(ImmutableSet::stream);
    }

    public abstract Stream<ImmutableSet<T>> executeStreamBatch(DynamoDbClient dynamo);
}
