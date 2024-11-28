package io.dataspray.singletable.builder;

import com.google.common.collect.ImmutableSet;
import io.dataspray.singletable.ExpressionBuilder;
import io.dataspray.singletable.Schema;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

@Slf4j
public class UpdateBuilder<T> extends ExpressionBuilder<T, UpdateBuilder<T>, UpdateItemRequest.Builder> implements ConditionExpressionBuilder<UpdateBuilder<T>>, UpdateExpressionBuilder<T, UpdateBuilder<T>> {

    public UpdateBuilder(Schema<T> schema) {
        super(schema);
    }

    protected UpdateBuilder<T> getParent() {
        return this;
    }

    private Optional<Map<String, AttributeValue>> keyOpt = Optional.empty();
    private boolean upsertCalled = false;
    private boolean conditionExistsCalled = false;
    private boolean suppressNonExistentValidation = false;

    public UpdateBuilder<T> key(Map<String, Object> primaryKey) {
        this.keyOpt = Optional.of(schema.primaryKey(primaryKey));
        return this;
    }

    @Override
    public UpdateBuilder<T> upsert(T item, ImmutableSet<String> skipFieldNames) {
        this.upsertCalled = true;
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
        validateNonExistent();
        return dynamo.updateItem(build());
    }

    public T executeGetUpdated(DynamoDbClient dynamo) {
        validateNonExistent();
        return checkNotNull(schema.fromAttrMap(dynamo.updateItem(builder()
                        .returnValues(ReturnValue.ALL_NEW)
                        .build()).attributes()),
                // This is a weird edge case, this can happen only if:
                // - You didn't have conditionExists() and the item didn't already exist
                // - You didn't actually update any fields in this update
                "No updated item returned");
    }

    public Optional<T> executeGetPrevious(DynamoDbClient dynamo) {
        validateNonExistent();
        return Optional.ofNullable(schema.fromAttrMap(dynamo.updateItem(builder()
                .returnValues(ReturnValue.ALL_OLD)
                .build()).attributes()));
    }

    @Override
    public UpdateBuilder<T> conditionExists() {
        this.conditionExistsCalled = true;
        return super.conditionExists();
    }

    /**
     * Suppresses warning to check whether an item is being updated without a check that it exists.
     */
    public UpdateBuilder<T> suppressNonExistent() {
        this.suppressNonExistentValidation = true;
        return this;
    }

    void validateNonExistent() {
        if (!suppressNonExistentValidation && !conditionExistsCalled && !upsertCalled) {
            log.warn("It appears you are updating an item without calling conditionExists() or upsert()"
                    + " which may result in a corrupted entry missing required fields."
                    + " Suppress this warning with suppressNonExistent().");
        }
    }
}
