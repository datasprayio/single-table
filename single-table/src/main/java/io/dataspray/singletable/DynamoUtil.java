// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.amazonaws.services.dynamodbv2.document.BatchGetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Page;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.Uninterruptibles;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;

class DynamoUtil {

    private static final long START_MS = 100L;
    private static final long SLEEP_MULTIPLE = 4L;

    private final DynamoDB dynamoDoc;

    DynamoUtil(DynamoDB dynamoDoc) {
        this.dynamoDoc = dynamoDoc;
    }

    public Stream<Item> retryUnprocessed(BatchGetItemOutcome outcome) {
        Stream<Item> items = outcome.getTableItems()
                .values()
                .stream()
                .flatMap(Collection::stream);
        if (outcome.getUnprocessedKeys().isEmpty()) {
            return items;
        }

        long sleepNext = START_MS;
        do {
            Uninterruptibles.sleepUninterruptibly(sleepNext, TimeUnit.MILLISECONDS);
            sleepNext *= SLEEP_MULTIPLE;

            outcome = dynamoDoc.batchGetItemUnprocessed(outcome.getUnprocessedKeys());
            if (!outcome.getTableItems().isEmpty()) {
                items = Stream.concat(items, outcome.getTableItems()
                        .values()
                        .stream()
                        .flatMap(Collection::stream));
            }
        } while (!outcome.getUnprocessedKeys().isEmpty());

        return items;
    }

    public void retryUnprocessed(BatchWriteItemOutcome outcome) {
        long sleepNext = START_MS;
        while (!outcome.getUnprocessedItems().isEmpty()) {
            Uninterruptibles.sleepUninterruptibly(sleepNext, TimeUnit.MILLISECONDS);
            sleepNext *= SLEEP_MULTIPLE;

            outcome = dynamoDoc.batchWriteItemUnprocessed(outcome.getUnprocessedItems());
        }
    }

    public <T> ShardPageResult<T> fetchShardNextPage(Schema<T> schema, Optional<String> cursorOpt, int maxPageSize) {
        return fetchShardNextPage(schema, cursorOpt, maxPageSize, Map.of());
    }

    public <T> ShardPageResult<T> fetchShardNextPage(Schema<T> schema, Optional<String> cursorOpt, int maxPageSize, Map<String, Object> values) {
        checkArgument(maxPageSize > 0, "Max page size must be greater than zero");
        Optional<ShardAndExclusiveStartKey> shardAndExclusiveStartKeyOpt = cursorOpt.map(schema::toShardedExclusiveStartKey);
        ImmutableList.Builder<T> itemsBuilder = ImmutableList.builder();
        do {
            int shard = shardAndExclusiveStartKeyOpt.map(ShardAndExclusiveStartKey::getShard).orElse(0);
            Page<Item, QueryOutcome> page = schema.queryApi().query(new QuerySpec()
                            .withHashKey(schema.shardKey(shard, values))
                            .withMaxPageSize(maxPageSize)
                            .withExclusiveStartKey(shardAndExclusiveStartKeyOpt
                                    .flatMap(ShardAndExclusiveStartKey::getExclusiveStartKey)
                                    .orElse(null)))
                    .firstPage();
            shardAndExclusiveStartKeyOpt = Optional.ofNullable(page.getLowLevelResult().getQueryResult().getLastEvaluatedKey())
                    .map(lastEvaluatedKey -> schema.wrapShardedLastEvaluatedKey(Optional.of(lastEvaluatedKey), shard))
                    .or(() -> shard < (schema.shardCount() - 1)
                            ? Optional.of(schema.wrapShardedLastEvaluatedKey(Optional.empty(), shard + 1))
                            : Optional.empty());
            ImmutableList<T> nextItems = page.getLowLevelResult().getItems().stream()
                    .map(schema::fromItem)
                    .collect(ImmutableList.toImmutableList());
            maxPageSize -= nextItems.size();
            itemsBuilder.addAll(nextItems);
        } while (maxPageSize > 0 && shardAndExclusiveStartKeyOpt.isPresent());
        return new ShardPageResult<T>(
                itemsBuilder.build(),
                shardAndExclusiveStartKeyOpt.map(schema::serializeShardedLastEvaluatedKey));
    }

    public static int deterministicPartition(String input, int partitionCount) {
        return Math.abs(Hashing.murmur3_32().hashString(input, Charsets.UTF_8).asInt() % partitionCount);
    }
}
