// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

class DynamoUtil {

    public <T> ShardPageResult<T> fetchShardNextPage(DynamoDbClient client, Schema<T> schema, Optional<String> cursorOpt, int maxPageSize) {
        return fetchShardNextPage(client, schema, cursorOpt, maxPageSize, Map.of());
    }

    public <T> ShardPageResult<T> fetchShardNextPage(DynamoDbClient client, Schema<T> schema, Optional<String> cursorOpt, int maxPageSize, Map<String, Object> values) {
        checkArgument(maxPageSize > 0, "Max page size must be greater than zero");
        Optional<ShardAndExclusiveStartKey> shardAndExclusiveStartKeyOpt = cursorOpt.map(schema::toShardedExclusiveStartKey);
        ImmutableList.Builder<T> itemsBuilder = ImmutableList.builder();
        do {
            int shard = shardAndExclusiveStartKeyOpt.map(ShardAndExclusiveStartKey::getShard).orElse(0);
            QueryRequest.Builder queryBuilder = QueryRequest.builder()
                    .tableName(schema.tableName());
            schema.indexNameOpt().ifPresent(queryBuilder::indexName);
            QueryResponse page = client.query(queryBuilder
                    .keyConditions(schema.attrMapToConditions(schema.shardKey(shard, values)))
                    .limit(maxPageSize)
                    .exclusiveStartKey(shardAndExclusiveStartKeyOpt
                            .flatMap(ShardAndExclusiveStartKey::getExclusiveStartKey)
                            .orElse(null))
                    .build());
            shardAndExclusiveStartKeyOpt = (page.hasLastEvaluatedKey() ? Optional.of(page.lastEvaluatedKey()) : Optional.<Map<String, AttributeValue>>empty())
                    .map(lastEvaluatedKey -> schema.wrapShardedLastEvaluatedKey(Optional.of(lastEvaluatedKey), shard))
                    .or(() -> shard < (schema.shardCount() - 1)
                            ? Optional.of(schema.wrapShardedLastEvaluatedKey(Optional.empty(), shard + 1))
                            : Optional.empty());
            ImmutableList<T> nextItems = page.items().stream()
                    .map(schema::fromAttrMap)
                    .collect(ImmutableList.toImmutableList());
            maxPageSize -= nextItems.size();
            itemsBuilder.addAll(nextItems);
        } while (maxPageSize > 0 && shardAndExclusiveStartKeyOpt.isPresent());
        return new ShardPageResult<T>(
                itemsBuilder.build(),
                shardAndExclusiveStartKeyOpt.map(schema::serializeShardedLastEvaluatedKey));
    }

    public static int deterministicPartition(String input, int partitionCount) {
        return Math.abs(Hashing.murmur3_32_fixed().hashString(input, Charsets.UTF_8).asInt() % partitionCount);
    }
}
