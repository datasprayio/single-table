// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import io.dataspray.singletable.DynamoConvertersProxy.OverrideCollectionTypeConverter;
import io.dataspray.singletable.DynamoConvertersProxy.OverrideTypeConverter;
import io.dataspray.singletable.builder.QueryBuilder;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.dataspray.singletable.TableType.Gsi;
import static io.dataspray.singletable.TableType.Primary;
import static org.junit.Assert.assertEquals;

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
                .tableName(tableName).build();
        TableSchema<Account> schema = singleTable.parseTableSchema(Account.class);

        // Insert new account
        Account account = schema.put()
                .item(new Account("8426", "matus@example.com", null))
                .executeGetNew(client);

        // Fetch other account
        Optional<Account> otherAccountOpt = schema.get()
                .key(Map.of("accountId", "abc-9473"))
                .executeGet(client);
    }

    @Test(timeout = 20_000L)
    public void testInitialize() throws Exception {
        DynamoDbClient dynamo = client;

        SingleTable singleTable = SingleTable.builder()
                .tableName(tableName)
                .build();
        TableSchema<Account> accountSchema = singleTable.parseTableSchema(Account.class);
        IndexSchema<Account> accountByApiKeySchema = singleTable.parseGlobalSecondaryIndexSchema(1, Account.class);
    }

    @Test(timeout = 20_000L)
    public void testInsert() throws Exception {
        Account account = new Account("12345", "email@example.com", "api-key");
        TableSchema<Account> schema = singleTable.parseTableSchema(Account.class);

        // ---------

        Optional<Account> previousAccount = schema.put()
                .item(account)
                .executeGetPrevious(client);
    }

    @Test(timeout = 20_000L)
    public void testPut() throws Exception {
        String apiKey = "api-key";
        String entryId = "entry-id";
        TableSchema<Account> schema = singleTable.parseTableSchema(Account.class);

        // ---------

        Account updatedAccount = schema.update()
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

                .executeGetUpdated(client);
    }

    @Test(timeout = 20_000L)
    public void testGet() throws Exception {
        TableSchema<Account> schema = singleTable.parseTableSchema(Account.class);

        // ---------

        Optional<Account> accountOpt = schema.get()
                .key(Map.of("accountId", "12345"))
                .executeGet(client);
    }

    @Test(timeout = 20_000L)
    public void testDelete() throws Exception {
        TableSchema<Account> schema = singleTable.parseTableSchema(Account.class);

        // ---------

        Optional<Account> deletedAccountOpt = schema.delete()
                .key(Map.of("accountId", "12345"))
                .executeGetDeleted(client);
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

        List<Account> accounts = schema.query()
                // Query by partition key
                .keyConditionsEqualsPrimaryKey(Map.of("accountId", "12345"))
                .executeStream(client)
                .collect(Collectors.toList());
    }

    @Test(timeout = 20_000L)
    public void testQueryOld() throws Exception {
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
                .executeGet(client);

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

    @Value
    @AllArgsConstructor
    static class MyClass {
        @NonNull
        String field1;
        @NonNull
        Long field2;
    }

    @Value
    @AllArgsConstructor
    @DynamoTable(type = Primary, partitionKeys = "accountId", rangePrefix = "account")
    static class MyClassHolder {
        @NonNull
        String accountId;

        @NonNull
        MyClass myClass;
    }

    @Test(timeout = 20_000L)
    public void testCustomMarshaller() throws Exception {

        class MyClassConverter implements OverrideTypeConverter<MyClass> {

            @Override
            public Class<?> getTypeClass() {
                return MyClass.class;
            }

            @Override
            public MyClass getDefaultInstance() {
                return new MyClass("", 0L);
            }

            @Override
            public AttributeValue marshall(MyClass myClass) {
                return AttributeValue.fromM(Map.of(
                        "field1", AttributeValue.fromS(myClass.field1),
                        "field2", AttributeValue.fromN(myClass.field2.toString())
                ));
            }

            @Override
            public MyClass unmarshall(AttributeValue attributeValue) {
                return new MyClass(
                        attributeValue.m().get("field1").s(),
                        Long.valueOf(attributeValue.m().get("field2").n()));
            }
        }

        SingleTable singleTable = SingleTable.builder()
                .tableName(tableName)
                .overrideTypeConverters(List.of(new MyClassConverter()))
                .build();

        TableSchema<MyClassHolder> schema = singleTable.parseTableSchema(MyClassHolder.class);

        MyClassHolder holder = schema.put()
                .item(new MyClassHolder("123", new MyClass("asfd", 19L)))
                .executeGetNew(client);
        Optional<MyClassHolder> holderActualOpt = schema.get()
                .key(Map.of("accountId", "123"))
                .executeGet(client);
        assertEquals(Optional.of(holder), holderActualOpt);
    }

    @Value
    @AllArgsConstructor
    @DynamoTable(type = Primary, partitionKeys = "accountId", rangePrefix = "account")
    static class BiMapHolder {
        @NonNull
        String accountId;

        @NonNull
        BiMap<String, Long> map;
    }

    @Test(timeout = 20_000L)
    public void testCustomCollectionMarshaller() throws Exception {

        class BiMapConverter implements OverrideCollectionTypeConverter<BiMap> {

            @Override
            public Class<?> getCollectionClass() {
                return BiMap.class;
            }

            @Override
            public AttributeValue marshall(BiMap object, DynamoConvertersProxy.MarshallerAttrVal<Object> marshaller) {
                return object == null ? null : AttributeValue.fromM(((Map<?, ?>) object).entrySet().stream()
                        .collect(ImmutableBiMap.toImmutableBiMap(
                                e -> (String) e.getKey(),
                                e -> marshaller.marshall(e.getValue())
                        )));
            }

            @Override
            public BiMap unmarshall(AttributeValue attributeValue, DynamoConvertersProxy.UnMarshallerAttrVal<Object> unMarshallerAttrVal) {
                return attributeValue == null || Boolean.TRUE.equals(attributeValue.nul()) || !attributeValue.hasM() ? null : attributeValue.m().entrySet().stream()
                        .collect(ImmutableBiMap.toImmutableBiMap(
                                Map.Entry::getKey,
                                e -> unMarshallerAttrVal.unmarshall(e.getValue())
                        ));
            }

            @Override
            public BiMap getDefaultInstance() {
                return ImmutableBiMap.of();
            }
        }

        SingleTable singleTable = SingleTable.builder()
                .tableName(tableName)
                .overrideCollectionTypeConverters(List.of(new BiMapConverter()))
                .build();

        TableSchema<BiMapHolder> schema = singleTable.parseTableSchema(BiMapHolder.class);

        BiMapHolder holder = schema.put()
                .item(new BiMapHolder("123", ImmutableBiMap.of("a", 1L, "b", 2L)))
                .executeGetNew(client);
        Optional<BiMapHolder> holderActualOpt = schema.get()
                .key(Map.of("accountId", "123"))
                .executeGet(client);
        assertEquals(Optional.of(holder), holderActualOpt);
    }
}
