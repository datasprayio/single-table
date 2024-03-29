// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.Optional;
import java.util.function.Supplier;

@Slf4j
public class InMemoryDynamoDbProvider implements Supplier<DynamoDbClient> {

    private Optional<AmazonDynamoDBLocal> amazonDynamoDBLocalOpt = Optional.empty();
    private Optional<DynamoDbClient> amazonDynamoDBOpt = Optional.empty();

    @Override
    public DynamoDbClient get() {
        System.setProperty("sqlite4java.library.path", "target/native-lib");
        amazonDynamoDBLocalOpt = Optional.of(DynamoDBEmbedded.create());
        amazonDynamoDBOpt = Optional.of(amazonDynamoDBLocalOpt.get().dynamoDbClient());
        return amazonDynamoDBOpt.get();
    }
}
