// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.google.common.collect.ImmutableMap;
import io.dataspray.singletable.builder.QueryBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Condition;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

public interface Schema<T> {
    String tableName();


    QueryBuilder<T> query();


    Map<String, AttributeValue> primaryKey(T obj);

    Map<String, AttributeValue> primaryKey(Map<String, Object> values);

    String partitionKeyName();

    Entry<String, AttributeValue> partitionKey(T obj);

    Entry<String, AttributeValue> partitionKey(Map<String, Object> values);

    Map<String, Condition> attrMapToConditions(Entry<String, AttributeValue> attrEntry);

    Map<String, Condition> attrMapToConditions(Map<String, AttributeValue> attrMap);

    /**
     * Use if shardKeys are set but partitionKeys are empty
     */
    Entry<String, AttributeValue> shardKey(int shard);

    /**
     * Use if shardKeys and partitionKeys are both needed
     */
    Entry<String, AttributeValue> shardKey(int shard, Map<String, Object> values);

    String partitionKeyValue(T obj);

    String partitionKeyValue(Map<String, Object> values);

    String rangeKeyName();

    Entry<String, AttributeValue> rangeKey(T obj);

    Entry<String, AttributeValue> rangeKey(Map<String, Object> values);

    /**
     * Retrieve sort key from incomplete given values.
     * Intended to be used by a range query.
     */
    Entry<String, AttributeValue> rangeKeyPartial(Map<String, Object> values);

    /**
     * Retrieve sort key value from incomplete given values.
     * Intended to be used by a range query.
     */
    String rangeValuePartial(Map<String, Object> values);


    AttributeValue toAttrValue(Object object);

    AttributeValue toAttrValue(String fieldName, Object object);

    Object fromAttrValue(String fieldName, AttributeValue attrVal);

    ImmutableMap<String, AttributeValue> toAttrMap(T obj);

    T fromAttrMap(Map<String, AttributeValue> attrMap);


    /**
     * Shard count or -1 if shardKeys are not set
     */
    int shardCount();


    Optional<String> serializeLastEvaluatedKey(Map<String, AttributeValue> lastEvaluatedKey);

    Map<String, AttributeValue> toExclusiveStartKey(String serializedlastEvaluatedKey);

    String serializeShardedLastEvaluatedKey(Optional<Map<String, AttributeValue>> lastEvaluatedKeyOpt, int shard);

    ShardAndExclusiveStartKey wrapShardedLastEvaluatedKey(Optional<Map<String, AttributeValue>> lastEvaluatedKeyOpt, int shard);

    String serializeShardedLastEvaluatedKey(ShardAndExclusiveStartKey shardAndExclusiveStartKey);

    ShardAndExclusiveStartKey toShardedExclusiveStartKey(String serializedShardedLastEvaluatedKey);


    /**
     * If this is an IndexSchema, returns the name of the index.
     */
    @Deprecated
    Optional<String> indexNameOpt();
}
