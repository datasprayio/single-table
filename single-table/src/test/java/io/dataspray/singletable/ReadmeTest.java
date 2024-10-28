// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.google.common.collect.ImmutableList;
import io.dataspray.singletable.builder.QueryBuilder;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.Map;
import java.util.Optional;

import static io.dataspray.singletable.TableType.Gsi;
import static io.dataspray.singletable.TableType.Primary;

@Slf4j
public class ReadmeTest extends AbstractDynamoTest {

    @Builder(toBuilder = true)
    @Value
    @AllArgsConstructor
    @DynamoTable(type = Primary, partitionKeys = "accountId", rangePrefix = "account")
    @DynamoTable(type = Gsi, indexNumber = 1, partitionKeys = {"apiKey"}, rangePrefix = "accountByApiKey")
    @DynamoTable(type = Gsi, indexNumber = 2, partitionKeys = {"email"}, rangePrefix = "accountByEmail")
    static class Account {
        @NonNull
        String accountId;

        @NonNull
        String email;

        @ToString.Exclude
        String apiKey;
    }

    @Test(timeout = 20_000L)
    public void testExample() throws Exception {

        // Initialize schema
        SingleTable singleTable = SingleTable.builder()
                .tablePrefix(tablePrefix).build();
        TableSchema<Account> schema = singleTable.parseTableSchema(Account.class);

        // Insert new account
        Account account = new Account("8426", "matus@example.com", null);
        schema.put().item(account).execute(client);

        // Fetch other account
        Optional<Account> otherAccountOpt = schema.get()
                .key(Map.of("accountId", "abc-9473"))
                .execute(client);
    }

    @Test(timeout = 20_000L)
    public void testInitialize() throws Exception {
        DynamoDbClient dynamo = client;

        SingleTable singleTable = SingleTable.builder()
                .tablePrefix(tablePrefix)
                .build();
        TableSchema<Account> accountSchema = singleTable.parseTableSchema(Account.class);
        IndexSchema<Account> accountByApiKeySchema = singleTable.parseGlobalSecondaryIndexSchema(1, Account.class);
    }

    @Test(timeout = 20_000L)
    public void testInsert() throws Exception {
        Account account = new Account("12345", "email@example.com", "api-key");
        TableSchema<Account> schema = singleTable.parseTableSchema(Account.class);

        // ---------

        schema.put()
                .item(account)
                .execute(client);
    }

    @Test(timeout = 20_000L)
    public void testPut() throws Exception {
        String apiKey = "api-key";
        String entryId = "entry-id";
        TableSchema<Account> schema = singleTable.parseTableSchema(Account.class);

        // ---------

        Optional<Account> updatedAccountOpt = schema.update()
                .key(Map.of("accountId", "1234fsd432e5"))

                // Apply conditions
                .conditionNotExists()
                .conditionFieldNotExists("email")
                //                .conditionFieldNotEquals("apiKey", "123")

                // Modify data
                .set("accountId", "asfasdf")
                .set("email", "grfgerfg")
                .setIncrement("votersCount", 1)
                // Remove entry from a json field
                //                .remove(ImmutableList.of("entryJson", entryId, "isMoved"))

                .execute(client);
    }

    @Test(timeout = 20_000L)
    public void testGet() throws Exception {
        TableSchema<Account> schema = singleTable.parseTableSchema(Account.class);

        // ---------

        Optional<Account> accountOpt = schema.get()
                .key(Map.of("accountId", "12345"))
                .execute(client);
    }

    @Test(timeout = 20_000L)
    public void testDelete() throws Exception {
        TableSchema<Account> schema = singleTable.parseTableSchema(Account.class);

        // ---------

        Optional<Account> deletedAccountOpt = schema.delete()
                .key(Map.of("accountId", "12345"))
                .execute(client);
    }

    @FunctionalInterface
    interface Processor {
        void process(Account account);
    }

    @Test(timeout = 20_000L)
    public void testQuery() throws Exception {
        TableSchema<Account> schema = singleTable.parseTableSchema(Account.class);
        Processor processor = account -> log.info("Account: {}", account);

        // ---------

        Optional<String> cursor = Optional.empty();
        do {
            // Prepare request
            QueryBuilder<Account> builder = schema.query()
                    // Query by partition key
                    .keyConditionsEqualsPrimaryKey(Map.of("accountId", "12345"))
                    .builder(b -> b.limit(2));
            cursor.ifPresent(exclusiveStartKey -> builder.builder(b -> b
                    .exclusiveStartKey(schema.toExclusiveStartKey(exclusiveStartKey))));

            // Perform request
            QueryResponse response = builder.execute(client);

            // Retrieve next cursor
            cursor = schema.serializeLastEvaluatedKey(response.lastEvaluatedKey());

            // Process results
            response.items().stream()
                    .map(schema::fromAttrMap)
                    .forEachOrdered(this::processAccount);
        } while (cursor.isPresent());
    }

    private void processAccount(Account account) {
    }

    @Value
    @AllArgsConstructor
    @DynamoTable(type = Primary, shardKeys = {"catId"}, shardPrefix = "cat", shardCount = 100, rangePrefix = "cat", rangeKeys = "catId")
    public static class Cat {
        @NonNull
        String catId;
    }

    @Test(timeout = 20_000L)
    public void testScanType() throws Exception {

        TableSchema<Cat> schema = singleTable.parseTableSchema(Cat.class);

        // ---------

        String catId = "A18D5B00";
        Cat myCat = new Cat(catId);

        // Insertion is same as before, sharding is done under the hood
        schema.put()
                .item(myCat)
                .execute(client);

        // Retrieving cat is also same as before
        Optional<Cat> catOpt = schema.get()
                .key(Map.of("catId", catId))
                .execute(client);

        // Finally let's dump all our cats using pagination
        Optional<String> cursorOpt = Optional.empty();
        do {
            ShardPageResult<Cat> result = singleTable.fetchShardNextPage(
                    client,
                    schema,
                    /* Pagination token */ cursorOpt,
                    /* page size */ 100);
            cursorOpt = result.getCursorOpt();
            processCats(result.getItems());
        } while (cursorOpt.isPresent());
    }

    private void processCats(ImmutableList<Cat> cats) {
    }


    @Test(timeout = 20_000L)
    public void testUpsert() throws Exception {
        TableSchema<Account> schema = singleTable.parseTableSchema(Account.class);

        // ---------

        Account account = new Account("12345", "asda@example.com", "api-key");

        // Upsert -- create it
        schema.update()
                .upsert(account)
                .execute(client);

        // Upsert -- update it
        schema.update()
                .upsert(account.toBuilder().apiKey("new-key").build())
                .execute(client);
    }
}