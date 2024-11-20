// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.dampcake.gson.immutable.ImmutableAdapterFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.Builder;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.constructs.Construct;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

public class SingleTable implements DynamoMapper {
    public static final String TTL_IN_EPOCH_SEC_ATTR_NAME = "ttlInEpochSec";

    @VisibleForTesting
    DynamoMapperImpl mapper;
    @VisibleForTesting
    DynamoUtil util;

    @Builder
    private SingleTable(@Nullable String tableName, @Deprecated @Nullable String tablePrefix, Gson overrideGson) {
        if (Strings.isNullOrEmpty(tablePrefix) == Strings.isNullOrEmpty(tableName)) {
            throw new RuntimeException("Must specify either tableName or tablePrefix");
        }
        Gson gson = overrideGson != null ? overrideGson : new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.IDENTITY)
                .disableHtmlEscaping()
                .registerTypeAdapterFactory(ImmutableAdapterFactory.forGuava())
                .create();
        this.mapper = new DynamoMapperImpl(tableName, tablePrefix, gson);
        this.util = new DynamoUtil();
    }

    @Override
    public String getTableName() {
        return mapper.getTableName();
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

    public <T> ShardPageResult<T> fetchShardNextPage(DynamoDbClient client, Schema<T> schema, Optional<String> cursorOpt, int maxPageSize, Map<String, Object> keyConditions) {
        return util.fetchShardNextPage(client, schema, cursorOpt, maxPageSize, keyConditions);
    }

    public <T> ShardPageResult<T> fetchShardNextPage(DynamoDbClient client, Schema<T> schema, Optional<String> cursorOpt, int maxPageSize, Map<String, Object> keyConditions, Consumer<QueryRequest.Builder> queryRequestConsumer) {
        return util.fetchShardNextPage(client, schema, cursorOpt, maxPageSize, keyConditions, queryRequestConsumer);
    }

    public int deterministicPartition(String input, int partitionCount) {
        return DynamoUtil.deterministicPartition(input, partitionCount);
    }
}
