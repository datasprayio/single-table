package io.dataspray.singletable.builder;

import io.dataspray.singletable.ExpressionBuilder;
import io.dataspray.singletable.Schema;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class GetBuilder<T> extends ExpressionBuilder<T, GetBuilder<T>> {

    public GetBuilder(Schema<T> schema) {
        super(schema);
    }

    protected GetBuilder<T> getParent() {
        return this;
    }

    private Optional<Map<String, AttributeValue>> keyOpt = Optional.empty();

    public GetBuilder<T> key(Map<String, Object> primaryKey) {
        checkState(!built);
        this.keyOpt = Optional.of(schema.primaryKey(primaryKey));
        return this;
    }

    public GetItemRequest.Builder builder() {
        GetItemRequest.Builder builder = GetItemRequest.builder();
        builder.tableName(schema.tableName());
        keyOpt.ifPresent(builder::key);
        return builder;
    }

    public GetItemRequest build() {
        return builder().build();
    }

    public Optional<T> execute(DynamoDbClient dynamo) {
        return Optional.ofNullable(schema.fromAttrMap(dynamo
                .getItem(build())
                .item()));
    }
}
