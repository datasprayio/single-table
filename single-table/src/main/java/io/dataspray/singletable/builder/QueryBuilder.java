package io.dataspray.singletable.builder;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import io.dataspray.singletable.Schema;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class QueryBuilder<T> extends CombinedQueryBuilder<T, QueryBuilder<T>> {

    protected Optional<Map<String, Condition>> keyConditionsOpt = Optional.empty();

    public QueryBuilder(Schema<T> schema, Optional<String> indexNameOpt) {
        super(schema, indexNameOpt);
    }

    protected QueryBuilder<T> getParent() {
        return this;
    }

    public QueryBuilder<T> keyConditions(Map<String, Condition> conditions) {
        this.keyConditionsOpt = Optional.of(conditions);
        return getParent();
    }

    public QueryBuilder<T> keyConditionsEqualsPrimaryKey(Map<String, Object> primaryKey) {
        this.keyConditionsOpt = Optional.of(schema.attrMapToConditions(schema.primaryKey(primaryKey)));
        return getParent();
    }

    public QueryRequest.Builder builder() {
        QueryRequest.Builder builder = super.builder();
        keyConditionsOpt.ifPresent(builder::keyConditions);
        return builder;
    }

    public Stream<ImmutableSet<T>> executeStreamBatch(DynamoDbClient dynamo) {
        return Stream.generate(new Supplier<Optional<ImmutableSet<T>>>() {
                    private Optional<String> cursorOpt;

                    @Override
                    public Optional<ImmutableSet<T>> get() {
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
                                ? Optional.of(response.items().stream()
                                .map(schema::fromAttrMap)
                                .collect(ImmutableSet.toImmutableSet()))
                                : Optional.empty();
                    }
                })
                .takeWhile(Optional::isPresent)
                .map(Optional::orElseThrow);
    }
}
