# Single Table: a DynamoDB tool

- Make single-table design easy and error-prone
- Schema definition with serialization and deserialization
- Utility tools for expressions/conditions building, retries, paging, and more...

It's as simple as:

```java
@Value
@DynamoTable(type = Primary, partitionKeys = "accountId", rangePrefix = "account")
@DynamoTable(type = Gsi, indexNumber = 1, partitionKeys = {"apiKey"}, rangePrefix = "accountByApiKey")
@DynamoTable(type = Gsi, indexNumber = 2, partitionKeys = {"email"}, rangePrefix = "accountByEmail")
class Account {
    @NonNull
    String accountId;

    @NonNull
    String email;

    @ToString.Exclude
    String apiKey;
}

...

// Initialize schema
SingleTable singleTable = SingleTable.builder()
        .tableName("project").build();
TableSchema<Account> schema = singleTable.parseTableSchema(Account.class);

// Insert new account
Account account = schema.put()
        .item(new Account("8426", "matus@example.com", null))
        .executeGetNew(client);

// Fetch other account
Optional<Account> otherAccountOpt = schema.get()
        .key(Map.of("accountId", "abc-9473"))
        .executeGet(client);
```

Okay, it could be simpler...

## AWS SDK compatibility

We support both AWS SDK v1 and v2. The table below shows the compatibility matrix:

| Dynamo AWS SDK | SingleTable | Docs                                                                   |
|----------------|-------------|------------------------------------------------------------------------|
| Java 2.x       | 2.x.x       | This README                                                            |
| Java 1.x       | 0.x.x       | [0.x.x Branch](https://github.com/datasprayio/single-table/tree/0.x.x) |

## Installation

### Maven

```xml
<dependency>
    <groupId>io.dataspray</groupId>
    <artifactId>single-table</artifactId>
    <version>${single-table.version}</version>
</dependency>
```

Latest release in Maven Central is [here](https://search.maven.org/artifact/io.dataspray/single-table).

## Use cases

- [Getting started](#getting-started)
- [Create our table](#create-our-table)
- [Insert an item](#insert-an-item)
- [Update and Condition expressions builder](#update-and-condition-expressions-builder)
- [Select an item](#select-an-item)
- [Query ranges with paging](#query-ranges-with-paging)
- [Scan records of specific type](#scan-records-of-specific-type)
- [Upsert (Update or create if missing)](#upsert-update-or-create-if-missing))
- [Filter records](#filter-records)

### Getting started

In our examples, we skip the steps of initializing `SingleTable` and parsing our `schema`. Here is how you can do this:

```java
SingleTable singleTable = SingleTable.builder()
    .tableName("project")
    .build();
TableSchema<Account> accountSchema = singleTable.parseTableSchema(Account.class);
IndexSchema<Account> accountByApiKeySchema = singleTable.parseGlobalSecondaryIndexSchema(1, Account.class);
```

### Create our table

#### Via SDK

Our library assumes the table is created with partition and range keys with particular names. (`pk`, `sk`, `pkgsi1`,
...) Use our tool to create a valid table.

Note you need to indicate how many LSIs and GSIs you would like to create. This depends on how many you are using in
your schemas. But don't worry you can always add more later.

```java
singleTable.createTableIfNotExists(client, 2, 2);
```

#### Via CDK

Alternatively, you can create the DynamoDB table and all indexes via AWS CDK stack:

```java
singleTable.createCdkTable(this, "my-stack-name", 2, 2);
```

### Put an item

```java
Optional<Account> previousAccount = schema.put()
        .item(account)
        .executeGetPrevious(client);
```

### Get an item

```java
Optional<Account> accountOpt = schema.get()
        .key(Map.of("accountId", "12345"))
        .executeGet(client);
```

### Delete an item

```java
Optional<Account> deletedAccountOpt = schema.delete()
        .key(Map.of("accountId", "12345"))
        .executeGetDeleted(client);
```

### Update with conditions

```java
Account updatedAccount = schema.update()
        .key(Map.of("accountId", "12345"))

        // Apply conditions
        .conditionExists()
        .conditionFieldExists("cancelDate")
        .conditionFieldEquals("isCancelled", false)

        // Modify data
        .set("apiKey", apiKey)
        .setIncrement("votersCount", 1)
        // Add to a set
        .add("transactionIds", ImmutableSet.of("4234", "5312"))
        // Remove entry from a json field
        .remove(ImmutableList.of("entryJson", entryId, "isMoved"))

        .executeGetUpdated(client);
```

### Query ranges with paging

In this example, we will be querying all range keys for a given partition key.

On every request, we check if there are more results with `getLastEvaluatedKey` and then providing this cursor back
using `withExclusiveStartKey` to continue quering where we left off.

```java
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
            .forEachOrdered(processor::process);
} while (cursor.isPresent());
```

### Scan records of specific type

You may have Cats and Dogs inside your single-table design and you want to retrieve all the Cats without having to also
iterate over all the dogs.

One way to do this is using a DynamoDB technique
called [sharding](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-sharding.html). To
apply this technique to our Cats, instead of having a `catId` as the partition key, we will instead have `cat-XXX` where
XXX will be a deterministic shard partition number based on the `catId`. The `catId` will be stored as a range key
instead.

Our schema can look like this:

```java
    @DynamoTable(type = Primary, shardKeys = {"catId"}, shardPrefix = "cat", shardCount = 100, rangePrefix = "cat", rangeKeys = "catId")
    public class Cat {
        @NonNull String catId;
    }
```

And our usage would be:

```java
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
```

### Upsert (Update or create if missing)

Upserts are tricky in DynamoDB as there is no native support. Luckily we can do this ourselves by effectively
overwriting the entire record whether it exists or not and for particular fields, add logic how to compute the new value
based on previous value.

```java
Account account = new Account("12345", "asda@example.com", "api-key");

// Upsert -- create it
schema.update()
        .upsert(account)
        .execute(client);

// Upsert -- update it
schema.update()
        .upsert(account.toBuilder().apiKey("new-key").build())
        .execute(client);
```

In this case, we have first created an account and then updated the api key.

### Filter records

One way to retrieve a subset of records is to replicate them into a secondary index conditionally based on the existence
of a field.

Currently there isn't a way to do this with this library, but would be fairly trivial to add, contributions are welcome.

### Custom mapping of objects and collections

By default, any unknown object is serialized and deserialized using GSON. If you'd like to override this functionality,
you definitely can. This is especially useful if you want to have a structure that can be updated using update expressions.

#### Custom object

Let's say you have the following class:

```java
@Value
@AllArgsConstructor
static class MyClass {
    @NonNull
    String field1;
    @NonNull
    Long field2;
}
```

Create a converter:

```java
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
```

And pass it along during Single Table creation:

```java
SingleTable singleTable = SingleTable.builder()
        .tableName(tableName)
        .overrideTypeConverters(List.of(new MyClassConverter()))
        .build();
```

#### Custom collection

Let's say you want to use a custom Map such as Guava's `BiMap`. Create a converter:

```java
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
```

And pass it along during Single Table creation:

```java
SingleTable singleTable = SingleTable.builder()
        .tableName(tableName)
        .overrideCollectionTypeConverters(List.of(new BiMapConverter()))
        .build();
```

