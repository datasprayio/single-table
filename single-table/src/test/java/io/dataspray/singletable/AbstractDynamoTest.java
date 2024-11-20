// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import org.junit.Before;
import software.amazon.awscdk.Stack;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.UUID;

public class AbstractDynamoTest {
    protected final String tableName = "test-" + UUID.randomUUID();
    protected DynamoDbClient client;
    protected DynamoMapperImpl mapper;
    protected SingleTable singleTable;
    protected DynamoUtil util;

    @Before
    public void setupDynamoTest() {
        client = new InMemoryDynamoDbProvider().get();
        singleTable = SingleTable.builder()
                .tableName(tableName)
                .build();
        mapper = singleTable.mapper;
        util = singleTable.util;

        mapper.createTableIfNotExists(client, 2, 2);
        mapper.createCdkTable(new Stack(), "my-stack", 2, 2);
    }

    <T> T putData(TableSchema<T> schema, T data) {
        client.putItem(b -> b
                .tableName(schema.tableName())
                .item(schema.toAttrMap(data)));
        return data;
    }
}
