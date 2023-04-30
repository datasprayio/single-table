// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import org.junit.Before;
import software.amazon.awscdk.Stack;

import java.util.UUID;

public class AbstractDynamoTest {
    protected final String tablePrefix = "test-" + UUID.randomUUID().toString();
    protected AmazonDynamoDB dynamo;
    protected DynamoDB dynamoDoc;
    protected DynamoMapperImpl mapper;
    protected SingleTable singleTable;
    protected DynamoUtil util;

    @Before
    public void setupDynamoTest() {
        dynamo = new InMemoryDynamoDbProvider().get();
        singleTable = SingleTable.builder()
                .tablePrefix(tablePrefix)
                .overrideDynamo(dynamo)
                .build();
        mapper = singleTable.mapper;
        util = singleTable.util;

        mapper.createTableIfNotExists(2, 2);
        mapper.createCdkTable(new Stack(), "my-stack", 2, 2);
    }
}
