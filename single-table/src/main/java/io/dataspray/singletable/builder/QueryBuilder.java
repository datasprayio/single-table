package io.dataspray.singletable.builder;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import io.dataspray.singletable.ExpressionBuilder;
import io.dataspray.singletable.Schema;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.*;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;

public class QueryBuilder<T> extends ExpressionBuilder<T, QueryBuilder<T>, QueryRequest.Builder> implements FilterExpressionBuilder<QueryBuilder<T>>, ConditionExpressionBuilder<QueryBuilder<T>> {

    public QueryBuilder(Schema<T> schema) {
        super(schema);
    }

    protected QueryBuilder<T> getParent() {
        return this;
    }

    private Optional<Map<String, Condition>> keyConditionsOpt = Optional.empty();

    public QueryBuilder<T> keyConditions(Map<String, Condition> conditions) {
        this.keyConditionsOpt = Optional.of(conditions);
        return this;
    }

    public QueryBuilder<T> keyConditionsEqualsPrimaryKey(Map<String, Object> primaryKey) {
        this.keyConditionsOpt = Optional.of(schema.attrMapToConditions(schema.primaryKey(primaryKey)));
        return this;
    }

    public QueryRequest.Builder builder() {
        Expression<QueryRequest.Builder> expression = buildExpression();
        QueryRequest.Builder builder = QueryRequest.builder();
        builder.tableName(schema.tableName());
        schema.indexNameOpt().ifPresent(builder::indexName);
        checkState(expression.updateExpression().isEmpty(), "Query does not support update expression");
        expression.filterExpression().ifPresent(builder::filterExpression);
        expression.expressionAttributeNames().ifPresent(builder::expressionAttributeNames);
        expression.expressionAttributeValues().ifPresent(builder::expressionAttributeValues);
        keyConditionsOpt.ifPresent(builder::keyConditions);
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
        return Stream.generate(new Supplier<Optional<T>>() {
                    private Iterator<T> currentBatchIterator;
                    private Optional<String> cursorOpt;

                    @Override
                    public Optional<T> get() {
                        // Fetch if first call (is null)
                        // or results are depleted (nasNext is false) and next page is available (cursor present)
                        if (currentBatchIterator == null
                                || (!currentBatchIterator.hasNext() && cursorOpt.isPresent())) {
                            QueryRequest.Builder builder = builder();
                            if (cursorOpt != null && cursorOpt.isPresent()) {
                                builder.exclusiveStartKey(schema.toExclusiveStartKey(cursorOpt.get()));
                            }
                            QueryResponse response = dynamo.query(builder.build());
                            cursorOpt = schema.serializeLastEvaluatedKey(response.lastEvaluatedKey());
                            currentBatchIterator = response.items().stream()
                                    .map(schema::fromAttrMap)
                                    .iterator();
                        }

                        return currentBatchIterator.hasNext()
                                ? Optional.of(currentBatchIterator.next())
                                : Optional.empty();
                    }
                })
                .takeWhile(Optional::isPresent)
                .map(Optional::orElseThrow);
    }

    public Stream<List<T>> executeStreamBatch(DynamoDbClient dynamo) {
        return Stream.generate(new Supplier<Optional<List<T>>>() {
                    private Optional<String> cursorOpt;

                    @Override
                    public Optional<List<T>> get() {
                        if (cursorOpt != null && cursorOpt.isEmpty()) {
                            return Optional.empty();
                        }
                        QueryRequest.Builder builder = builder();
                        if (cursorOpt != null && cursorOpt.isPresent()) {
                            builder.exclusiveStartKey(schema.toExclusiveStartKey(cursorOpt.get()));
                        }
                        QueryResponse response = dynamo.query(builder.build());
                        cursorOpt = schema.serializeLastEvaluatedKey(response.lastEvaluatedKey());
                        return response.hasItems()
                                ? Optional.of(Collections.unmodifiableList(Lists.transform(response.items(), schema::fromAttrMap)))
                                : Optional.empty();
                    }
                })
                .takeWhile(Optional::isPresent)
                .map(Optional::orElseThrow);
    }
}
