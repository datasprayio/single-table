package io.dataspray.singletable.builder;

import io.dataspray.singletable.ExpressionBuilder;
import io.dataspray.singletable.Schema;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class GetBuilder<T> extends ExpressionBuilder<T, GetBuilder<T>, GetItemRequest.Builder> {

    public GetBuilder(Schema<T> schema) {
        super(schema);
    }

    protected GetBuilder<T> getParent() {
        return this;
    }

    private Optional<Map<String, AttributeValue>> keyOpt = Optional.empty();

    public GetBuilder<T> key(Map<String, Object> primaryKey) {
        this.keyOpt = Optional.of(schema.primaryKey(primaryKey));
        return getParent();
    }

    public GetItemRequest.Builder builder() {
        Expression<GetItemRequest.Builder> expression = buildExpression();
        GetItemRequest.Builder builder = GetItemRequest.builder();
        builder.tableName(schema.tableName());
        checkState(expression.updateExpression().isEmpty(), "Delete does not support update expression");
        checkState(expression.conditionExpression().isEmpty(), "Delete does not support condition/filter expression");
        keyOpt.ifPresent(builder::key);
        expression.builderAdjustments().forEach(c -> c.accept(builder));
        return builder;
    }

    public GetItemRequest build() {
        return builder().build();
    }

    public GetItemResponse execute(DynamoDbClient dynamo) {
        return dynamo.getItem(build());
    }

    public Optional<T> executeGet(DynamoDbClient dynamo) {
        return Optional.ofNullable(schema.fromAttrMap(dynamo
                .getItem(build())
                .item()));
    }
}
