package io.dataspray.singletable.builder;

import io.dataspray.singletable.ExpressionBuilder;
import io.dataspray.singletable.Schema;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class DeleteBuilder<T> extends ExpressionBuilder<T, DeleteBuilder<T>, DeleteItemRequest.Builder> implements ConditionExpressionBuilder<DeleteBuilder<T>> {

    public DeleteBuilder(Schema<T> schema) {
        super(schema);
    }

    protected DeleteBuilder<T> getParent() {
        return this;
    }

    private Optional<Map<String, AttributeValue>> keyOpt = Optional.empty();

    public DeleteBuilder<T> key(Map<String, Object> primaryKey) {
        this.keyOpt = Optional.of(schema.primaryKey(primaryKey));
        return getParent();
    }

    public DeleteItemRequest.Builder builder() {
        Expression<DeleteItemRequest.Builder> expression = buildExpression();
        DeleteItemRequest.Builder builder = DeleteItemRequest.builder();
        builder.tableName(schema.tableName());
        checkState(expression.updateExpression().isEmpty(), "Delete does not support update expression");
        expression.conditionExpression().ifPresent(builder::conditionExpression);
        expression.expressionAttributeNames().ifPresent(builder::expressionAttributeNames);
        expression.expressionAttributeValues().ifPresent(builder::expressionAttributeValues);
        keyOpt.ifPresent(builder::key);
        expression.builderAdjustments().forEach(c -> c.accept(builder));
        return builder;
    }

    public DeleteItemRequest build() {
        return builder().build();
    }

    public DeleteItemResponse execute(DynamoDbClient dynamo) {
        return dynamo.deleteItem(build());
    }

    public Optional<T> executeGetDeleted(DynamoDbClient dynamo) {
        return Optional.ofNullable(schema.fromAttrMap(dynamo.deleteItem(builder()
                .returnValues(ReturnValue.ALL_OLD)
                .build()).attributes()));
    }
}
