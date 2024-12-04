package io.dataspray.singletable.builder;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import io.dataspray.singletable.Schema;
import io.dataspray.singletable.ShardAndExclusiveStartKey;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static java.util.function.Predicate.not;

@Slf4j
public class ShardedQueryBuilder<T> extends CombinedQueryBuilder<T, ShardedQueryBuilder<T>> {

    protected Optional<Map<String, Object>> keyConditionValuesOpt = Optional.empty();

    public ShardedQueryBuilder(Schema<T> schema, Optional<String> indexNameOpt) {
        super(schema, indexNameOpt);
    }

    protected ShardedQueryBuilder<T> getParent() {
        return this;
    }

    public ShardedQueryBuilder<T> keyConditionValues(Map<String, Object> conditionValues) {
        this.keyConditionValuesOpt = Optional.of(conditionValues);
        return getParent();
    }

    @Override
    public QueryRequest build() {
        QueryRequest request = super.build();
        checkState(!request.keyConditions().isEmpty() || !request.keyConditionExpression().isEmpty(),
                "Sharded query requires key conditions, either use an .executeX() method that handles it for you, or set key conditions using .keyConditions(schema.attrMapToConditions(schema.shardKey(...)))");
        return request;
    }

    public Stream<ImmutableSet<T>> executeStreamBatch(DynamoDbClient dynamo) {
        return Stream.generate(new Supplier<Optional<ImmutableSet<T>>>() {
                    private Optional<ShardAndExclusiveStartKey> shardAndExclusiveStartKeyOpt;

                    @Override
                    public Optional<ImmutableSet<T>> get() {
                        Optional<ImmutableSet<T>> results = Optional.empty();
                        // If first time running (opt == null) or if cursor is present (opt is present), fetch data
                        if (shardAndExclusiveStartKeyOpt == null || shardAndExclusiveStartKeyOpt.isPresent()) {
                            if (shardAndExclusiveStartKeyOpt == null) {
                                shardAndExclusiveStartKeyOpt = Optional.empty();
                            }
                            int shard = shardAndExclusiveStartKeyOpt.map(ShardAndExclusiveStartKey::getShard).orElse(0);
                            Optional<Map<String, AttributeValue>> exclusiveStartKeyOpt = shardAndExclusiveStartKeyOpt.flatMap(ShardAndExclusiveStartKey::getExclusiveStartKey);
                            QueryResponse page = dynamo.query(builder()
                                    .keyConditions(schema.attrMapToConditions(schema.shardKey(shard, keyConditionValuesOpt.orElse(Map.of()))))
                                    .exclusiveStartKey(exclusiveStartKeyOpt.orElse(null))
                                    .build());
                            log.debug("Fetched shard {} startKey {} items {}", shard, exclusiveStartKeyOpt, page.count());
                            shardAndExclusiveStartKeyOpt = (page.hasLastEvaluatedKey() ? Optional.of(page.lastEvaluatedKey()) : Optional.<Map<String, AttributeValue>>empty())
                                    .map(lastEvaluatedKey -> schema.wrapShardedLastEvaluatedKey(Optional.of(lastEvaluatedKey), shard))
                                    .or(() -> shard < (schema.shardCount() - 1)
                                            ? Optional.of(schema.wrapShardedLastEvaluatedKey(Optional.empty(), shard + 1))
                                            : Optional.empty());
                            results = Optional.of(page.items().stream()
                                    .map(schema::fromAttrMap)
                                    .collect(ImmutableSet.toImmutableSet()));
                        }
                        return results;
                    }
                })
                .takeWhile(Optional::isPresent)
                .map(Optional::orElseThrow)
                .filter(not(ImmutableSet::isEmpty));
    }
}
