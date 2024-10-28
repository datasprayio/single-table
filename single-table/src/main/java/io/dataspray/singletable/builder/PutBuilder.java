package io.dataspray.singletable.builder;

import io.dataspray.singletable.ExpressionBuilder;
import io.dataspray.singletable.Schema;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class PutBuilder<T> extends ExpressionBuilder<T, PutBuilder<T>> implements ConditionExpressionBuilder<PutBuilder<T>> {

    public PutBuilder(Schema<T> schema) {
        super(schema);
    }

    protected PutBuilder<T> getParent() {
        return this;
    }

    private Optional<T> itemOpt = Optional.empty();

    public PutBuilder<T> item(T item) {
        checkState(!built);
        this.itemOpt = Optional.of(item);
        return this;
    }

    public PutItemRequest.Builder builder() {
        Expression expression = buildExpression();
        PutItemRequest.Builder builder = PutItemRequest.builder();
        builder.tableName(schema.tableName());
        checkState(expression.updateExpression().isEmpty(), "Put does not support update expression");
        expression.conditionExpression().ifPresent(builder::conditionExpression);
        expression.expressionAttributeNames().ifPresent(builder::expressionAttributeNames);
        expression.expressionAttributeValues().ifPresent(builder::expressionAttributeValues);
        itemOpt.ifPresent(item -> builder.item(schema.toAttrMap(item)));
        return builder;
    }

    public PutItemRequest build() {
        return builder().build();
    }

    public PutItemResponse execute(DynamoDbClient dynamo) {
        return dynamo.putItem(build());
    }
}
