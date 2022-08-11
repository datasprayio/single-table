// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.function.Supplier;

@Slf4j
public class InMemoryDynamoDbProvider implements Supplier<AmazonDynamoDB> {

    private Optional<AmazonDynamoDBLocal> amazonDynamoDBLocalOpt = Optional.empty();
    private Optional<AmazonDynamoDB> amazonDynamoDBOpt = Optional.empty();

    @Override
    public AmazonDynamoDB get() {
        System.setProperty("sqlite4java.library.path", "target/native-lib");
        amazonDynamoDBLocalOpt = Optional.of(DynamoDBEmbedded.create());
        amazonDynamoDBOpt = Optional.of(amazonDynamoDBLocalOpt.get().amazonDynamoDB());
        return amazonDynamoDBOpt.get();
    }
}
