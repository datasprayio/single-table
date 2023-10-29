// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import lombok.Builder;
import lombok.NonNull;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.constructs.Construct;

import java.util.Map;
import java.util.Optional;

public class SingleTable implements DynamoMapper {
    public static final String TTL_IN_EPOCH_SEC_ATTR_NAME = "ttlInEpochSec";

    @VisibleForTesting
    DynamoMapperImpl mapper;
    @VisibleForTesting
    DynamoUtil util;

    @Builder
    private SingleTable(@NonNull String tablePrefix, Gson overrideGson) {
        Gson gson = overrideGson != null ? overrideGson : new Gson();
        this.mapper = new DynamoMapperImpl(tablePrefix, gson);
        this.util = new DynamoUtil();
    }

    @Override
    public Table createCdkTable(Construct scope, String stackId, int lsiCount, int gsiCount) {
        return mapper.createCdkTable(scope, stackId, lsiCount, gsiCount);
    }

    @Override
    public void createTableIfNotExists(DynamoDbClient dynamo, int lsiCount, int gsiCount) {
        mapper.createTableIfNotExists(dynamo, lsiCount, gsiCount);
    }

    @Override
    public <T> TableSchema<T> parseTableSchema(Class<T> objClazz) {
        return mapper.parseTableSchema(objClazz);
    }

    @Override
    public <T> IndexSchema<T> parseLocalSecondaryIndexSchema(long indexNumber, Class<T> objClazz) {
        return mapper.parseLocalSecondaryIndexSchema(indexNumber, objClazz);
    }

    @Override
    public <T> IndexSchema<T> parseGlobalSecondaryIndexSchema(long indexNumber, Class<T> objClazz) {
        return mapper.parseGlobalSecondaryIndexSchema(indexNumber, objClazz);
    }

    public <T> ShardPageResult<T> fetchShardNextPage(DynamoDbClient client, Schema<T> schema, Optional<String> cursorOpt, int maxPageSize) {
        return util.fetchShardNextPage(client, schema, cursorOpt, maxPageSize);
    }

    public <T> ShardPageResult<T> fetchShardNextPage(DynamoDbClient client, Schema<T> schema, Optional<String> cursorOpt, int maxPageSize, Map<String, Object> values) {
        return util.fetchShardNextPage(client, schema, cursorOpt, maxPageSize, values);
    }

    public int deterministicPartition(String input, int partitionCount) {
        return DynamoUtil.deterministicPartition(input, partitionCount);
    }
}
