// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

public interface DynamoMapper {

    boolean createTableIfNotExists(int lsiCount, int gsiCount);

    <T> TableSchema<T> parseTableSchema(Class<T> objClazz);

    <T> IndexSchema<T> parseLocalSecondaryIndexSchema(long indexNumber, Class<T> objClazz);

    <T> IndexSchema<T> parseGlobalSecondaryIndexSchema(long indexNumber, Class<T> objClazz);
}
