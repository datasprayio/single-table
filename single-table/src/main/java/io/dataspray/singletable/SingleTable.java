// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.google.gson.Gson;
import lombok.Builder;
import lombok.NonNull;

public class SingleTable implements DynamoMapper {
    DynamoMapperImpl mapper;
    DynamoUtil util;

    @Builder
    private SingleTable(@NonNull DynamoDB dynamoDoc, @NonNull String tablePrefix, Gson overrideGson) {
        Gson gson = overrideGson != null ? overrideGson : new Gson();
        this.mapper = new DynamoMapperImpl(tablePrefix, gson, dynamoDoc);
        this.util = new DynamoUtil(dynamoDoc);
    }

    @Override
    public boolean createTableIfNotExists(int lsiCount, int gsiCount) {
        return mapper.createTableIfNotExists(lsiCount, gsiCount);
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

    public DynamoUtil util() {
        return util;
    }
}
