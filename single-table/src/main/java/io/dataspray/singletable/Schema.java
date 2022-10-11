// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.api.QueryApi;
import com.amazonaws.services.dynamodbv2.document.api.ScanApi;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Optional;

public interface Schema<T> {
    QueryApi queryApi();

    ScanApi scanApi();

    PrimaryKey primaryKey(T obj);

    PrimaryKey primaryKey(Map<String, Object> values);

    String partitionKeyName();

    KeyAttribute partitionKey(T obj);

    KeyAttribute partitionKey(Map<String, Object> values);

    /** Use if shardKeys are set but partitionKeys are empty */
    KeyAttribute shardKey(int shard);

    /** Use if shardKeys and partitionKeys are both needed */
    KeyAttribute shardKey(int shard, Map<String, Object> values);

    String partitionKeyValue(T obj);

    String partitionKeyValue(Map<String, Object> values);

    String rangeKeyName();

    KeyAttribute rangeKey(T obj);

    KeyAttribute rangeKey(Map<String, Object> values);

    /**
     * Retrieve sort key from incomplete given values.
     * Intended to be used by a range query.
     */
    KeyAttribute rangeKeyPartial(Map<String, Object> values);

    /**
     * Retrieve sort key value from incomplete given values.
     * Intended to be used by a range query.
     */
    String rangeValuePartial(Map<String, Object> values);


    Object toDynamoValue(String fieldName, Object object);

    Object fromDynamoValue(String fieldName, Object object);

    AttributeValue toAttrValue(String fieldName, Object object);

    Object fromAttrValue(String fieldName, AttributeValue attrVal);

    Item toItem(T obj);

    T fromItem(Item item);

    ImmutableMap<String, AttributeValue> toAttrMap(T obj);

    T fromAttrMap(Map<String, AttributeValue> attrMap);


    /** Shard count or -1 if shardKeys are not set */
    int shardCount();


    String upsertExpression(T object, Map<String, String> nameMap, Map<String, Object> valMap, ImmutableSet<String> skipFieldNames, String additionalExpression);

    String upsertExpressionAttrVal(T object, Map<String, String> nameMap, Map<String, AttributeValue> valMap, ImmutableSet<String> skipFieldNames, String additionalExpression);

    String serializeLastEvaluatedKey(Map<String, AttributeValue> lastEvaluatedKey);

    PrimaryKey toExclusiveStartKey(String serializedlastEvaluatedKey);

    String serializeShardedLastEvaluatedKey(Optional<Map<String, AttributeValue>> lastEvaluatedKeyOpt, int shard);

    ShardAndExclusiveStartKey wrapShardedLastEvaluatedKey(Optional<Map<String, AttributeValue>> lastEvaluatedKeyOpt, int shard);

    String serializeShardedLastEvaluatedKey(ShardAndExclusiveStartKey shardAndExclusiveStartKey);

    ShardAndExclusiveStartKey toShardedExclusiveStartKey(String serializedShardedLastEvaluatedKey);
}
