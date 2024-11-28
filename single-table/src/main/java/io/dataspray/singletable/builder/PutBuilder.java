package io.dataspray.singletable.builder;

import io.dataspray.singletable.ExpressionBuilder;
import io.dataspray.singletable.Schema;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class PutBuilder<T> extends ExpressionBuilder<T, PutBuilder<T>, PutItemRequest.Builder> implements ConditionExpressionBuilder<PutBuilder<T>> {

    public PutBuilder(Schema<T> schema) {
        super(schema);
    }

    protected PutBuilder<T> getParent() {
        return this;
    }

    private Optional<T> itemOpt = Optional.empty();

    public PutBuilder<T> item(T item) {
        this.itemOpt = Optional.of(item);
        return this;
    }

    public PutItemRequest.Builder builder() {
        Expression<PutItemRequest.Builder> expression = buildExpression();
        PutItemRequest.Builder builder = PutItemRequest.builder();
        builder.tableName(schema.tableName());
        checkState(expression.updateExpression().isEmpty(), "Put does not support update expression");
        expression.conditionExpression().ifPresent(builder::conditionExpression);
        expression.expressionAttributeNames().ifPresent(builder::expressionAttributeNames);
        expression.expressionAttributeValues().ifPresent(builder::expressionAttributeValues);
        checkState(itemOpt.isPresent(), "Need to call item(...) first");
        builder.item(schema.toAttrMap(itemOpt.get()));
        expression.builderAdjustments().forEach(c -> c.accept(builder));
        return builder;
    }

    public PutItemRequest build() {
        return builder().build();
    }

    public PutItemResponse execute(DynamoDbClient dynamo) {
        return dynamo.putItem(build());
    }

    public T executeGetNew(DynamoDbClient dynamo) {
        checkState(itemOpt.isPresent(), "Need to call item(...) first");
        execute(dynamo);
        return itemOpt.get();
    }

    public Optional<T> executeGetPrevious(DynamoDbClient dynamo) {
        return Optional.ofNullable(schema.fromAttrMap(dynamo.putItem(builder()
                .returnValues(ReturnValue.ALL_OLD)
                .build()).attributes()));
    }
}
