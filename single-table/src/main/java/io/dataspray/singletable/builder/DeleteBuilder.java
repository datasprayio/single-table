package io.dataspray.singletable.builder;

import io.dataspray.singletable.ExpressionBuilder;
import io.dataspray.singletable.Schema;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;

import static com.google.common.base.Preconditions.checkState;

public class DeleteBuilder<T> extends ExpressionBuilder<T, DeleteBuilder<T>> implements ConditionExpressionBuilder<DeleteBuilder<T>> {

    public DeleteBuilder(Schema<T> schema) {
        super(schema);
    }

    protected DeleteBuilder<T> getParent() {
        return this;
    }

    public DeleteItemRequest.Builder builder() {
        Expression expression = buildExpression();
        DeleteItemRequest.Builder builder = DeleteItemRequest.builder();
        builder.tableName(schema.tableName());
        checkState(expression.updateExpression().isEmpty(), "Delete does not support update expression");
        expression.conditionExpression().ifPresent(builder::conditionExpression);
        expression.expressionAttributeNames().ifPresent(builder::expressionAttributeNames);
        expression.expressionAttributeValues().ifPresent(builder::expressionAttributeValues);
        return builder;
    }

    public DeleteItemRequest build() {
        return builder().build();
    }
}
