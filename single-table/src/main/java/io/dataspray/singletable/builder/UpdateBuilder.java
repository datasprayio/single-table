package io.dataspray.singletable.builder;

import com.google.common.collect.ImmutableSet;
import io.dataspray.singletable.ExpressionBuilder;
import io.dataspray.singletable.Schema;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import java.util.Map;
import java.util.Optional;

public class UpdateBuilder<T> extends ExpressionBuilder<T, UpdateBuilder<T>, UpdateItemRequest.Builder> implements ConditionExpressionBuilder<UpdateBuilder<T>>, UpdateExpressionBuilder<T, UpdateBuilder<T>> {

    public UpdateBuilder(Schema<T> schema) {
        super(schema);
    }

    protected UpdateBuilder<T> getParent() {
        return this;
    }

    private Optional<Map<String, AttributeValue>> keyOpt = Optional.empty();

    public UpdateBuilder<T> key(Map<String, Object> primaryKey) {
        this.keyOpt = Optional.of(schema.primaryKey(primaryKey));
        return this;
    }

    @Override
    public UpdateBuilder<T> upsert(T item, ImmutableSet<String> skipFieldNames) {
        this.keyOpt = Optional.of(schema.primaryKey(item));
        return super.upsert(item, skipFieldNames);
    }


    public UpdateItemRequest.Builder builder() {
        Expression<UpdateItemRequest.Builder> expression = buildExpression();
        UpdateItemRequest.Builder builder = UpdateItemRequest.builder();
        builder.tableName(schema.tableName());
        expression.updateExpression().ifPresent(builder::updateExpression);
        expression.conditionExpression().ifPresent(builder::conditionExpression);
        expression.expressionAttributeNames().ifPresent(builder::expressionAttributeNames);
        expression.expressionAttributeValues().ifPresent(builder::expressionAttributeValues);
        keyOpt.ifPresent(builder::key);
        expression.builderAdjustments().forEach(c -> c.accept(builder));
        return builder;
    }

    public UpdateItemRequest build() {
        return builder().build();
    }

    public UpdateItemResponse execute(DynamoDbClient dynamo) {
        return dynamo.updateItem(build());
    }

    public Optional<T> executeGetUpdated(DynamoDbClient dynamo) {
        return Optional.ofNullable(schema.fromAttrMap(dynamo.updateItem(builder()
                .returnValues(ReturnValue.ALL_NEW)
                .build()).attributes()));
    }

    public Optional<T> executeGetPrevious(DynamoDbClient dynamo) {
        return Optional.ofNullable(schema.fromAttrMap(dynamo.updateItem(builder()
                .returnValues(ReturnValue.ALL_OLD)
                .build()).attributes()));
    }
}
