// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.constructs.Construct;

public interface DynamoMapper {

    String getTableName();

    Table createCdkTable(Construct scope, String stackId, int lsiCount, int gsiCount);

    void createTableIfNotExists(DynamoDbClient dynamo, int lsiCount, int gsiCount);

    <T> TableSchema<T> parseTableSchema(Class<T> objClazz);

    <T> IndexSchema<T> parseLocalSecondaryIndexSchema(long indexNumber, Class<T> objClazz);

    <T> IndexSchema<T> parseGlobalSecondaryIndexSchema(long indexNumber, Class<T> objClazz);
}
