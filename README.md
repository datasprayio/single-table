# Single Table - a DynamoDB tool

- Make single-table design easy and error-prone
- Schema definition with serialization and deserialization
- Utility tools for expressions/conditions building, retries, paging, and more...

It's as simple as:

```java
@Value
@DynamoTable(type = Primary, partitionKeys = "accountId", rangePrefix = "account")
@DynamoTable(type = Gsi, indexNumber = 1, partitionKeys = {"apiKey"}, rangePrefix = "accountByApiKey")
@DynamoTable(type = Gsi, indexNumber = 2, partitionKeys = {"oauthGuid"}, rangePrefix = "accountByOauthGuid")
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
        .dynamoDoc(dynamoDoc)
        .tablePrefix("project").build();
TableSchema<Account> schema = singleTable.parseTableSchema(Account.class);

// Insert new account
Account account = new Account("8426", "matus@example.com", null);
schema.table().putItem(new PutItemSpec().withItem(schema.toItem(account)));

// Fetch other account
Optional<Account> otherAccountOpt = Optional.ofNullable(schema.fromItem(schema.table().getItem(
        schema.primaryKey(Map.of("accountId","9473")))));
```

Okay, it could be simpler...

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
        .dynamoDoc(dynamoDoc)
        .tablePrefix("project").build();
TableSchema<Account> accountSchema=singleTable.parseTableSchema(Account.class);
IndexSchema<Account> accountByApiKeySchema=singleTable.parseGlobalSecondaryIndexSchema(1,Account.class);
```

### Create our table

Our library assumes the table is created with partition and range keys with particular names. (`pk`, `sk`, `pkgsi1`,
...) Use our tool to create a valid table.

Note you need to indicate how many LSIs and GSIs you would like to create. This depends on how many you are using in
your schemas. But don't worry you can always add more later.

```java
singleTable.createTableIfNotExists(2, 2);
```

### Insert an item

```java
schema.table().putItem(new PutItemSpec().withItem(schema.toItem(myAccount)));
```

### Update and Condition expressions builder

```java
ExpressionBuilder expressionBuilder = schema.expressionBuilder();

// Apply conditions
expressionBuilder
        // Item exists
        .conditionExists()
        // Particular field exists
        .conditionFieldExists("cancelDate")
        // Particular field equals a value
        .conditionFieldEquals("isCancelled", false);

// Modify data
expressionBuilder
        // Overwrite field
        .set("apiKey", apiKey)
        // Increment field value
        .setIncrement("votersCount", 1);
        // Add to a set
        .add("transactionIds", ImmutableSet.of("4234", "5312"))
        // Remove entry from a json field
        .remove(ImmutableList.of("entryJson", entryId, "isMoved"));

Expression expression = expressionBuilder.build();
...
    .withConditionExpression(expression.conditionExpression().orElse(null))
    .withUpdateExpression(expression.updateExpression().orElse(null))
    .withNameMap(expression.nameMap().orElse(null))
    .withValueMap(expression.valMap().orElse(null))
```

### Select an item

```java
Account account = schema.fromItem(schema.table().getItem(
        schema.primaryKey(Map.of("accountId","account-id-123"))));
```

You may want to wrap it in an optional if you prefer not to work with nulls:

```java
Optional<Account> accountOpt = Optional.ofNullable(schema.fromItem(schema.table().getItem(
        schema.primaryKey(Map.of("accountId","account-id-123")))));
```

### Query ranges with paging

In this example, we will be querying all range keys for a given partition key.

On every request, we check if there are more results with `getLastEvaluatedKey` and then providing this cursor back
using `withExclusiveStartKey` to continue quering where we left off.

```java
Optional<String> cursor = Optional.empty();
do {
    Page<Item, QueryOutcome> page = schema.table().query(new QuerySpec()
                    .withHashKey(schema.partitionKey(Map.of(
                            "accountId", accountId)))
                    .withRangeKeyCondition(new RangeKeyCondition(schema.rangeKeyName())
                            .beginsWith(schema.rangeValuePartial(Map.of())))
                    .withMaxPageSize(10)
                    .withScanIndexForward(false)
                    .withExclusiveStartKey(cursorOpt
                            .map(schema::toExclusiveStartKey)
                            .orElse(null)))
            .firstPage();
    cursor = Optional.ofNullable(page.getLowLevelResult()
                    .getQueryResult()
                    .getLastEvaluatedKey())
            .map(schema::serializeLastEvaluatedKey));
    ImmutableList<Account> results = page.getLowLevelResult()
            .getItems()
            .stream()
            .map(accountSchema::fromItem)
            .collect(ImmutableList.toImmutableList());
} while (cursor.isPresent());
```

### Scan records of specific type

You may have Cats and Dogs inside your single-table design and you want to retrieve all the Cat without having to also
iterate over all the dogs.

One way to do this is using a DynamoDB technique
called [sharding](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-sharding.html). To
apply this technique to our Cats, instead of having a `catId` as the parition key, we will instead have `cat-XXX` where
XXX will be a deterministic shard partition number based on the `catId`. The `catId` will be stored as a range key
instead.

Our schema can look like this:

```java
    @DynamoTable(type = Primary, partitionKeys = {"partition"}, rangePrefix = "cat", rangeKeys = "catId")
    public class Cat {
        @NonNull String catId;
        @NonNull long partition;
    }
```

And our usage would be:

```java
String catId = "A18D5B00";
// Deterministic purrtition number given catId and partition count of 8
int partition = singleTable.util().deterministicPartition(catId, 8);

Cat myCat = new Cat(catId, partition);

// Insertion is identical
schema.table().putItem(new PutItemSpec().withItem(schema.toItem(myCat)));

// Retrieving cat requires supplying the partition number
Cat otherCat = schema.fromItem(schema.table().getItem(
        schema.primaryKey(Map.of(
                "catId", catId,
                "partition", partition))));
```

### Upsert (Update or create if missing)

Upserts are tricky in DynamoDB as there is no native support. Luckily we can do this ourselves by effectively
overwriting the entire record whether it exists or not and for particular fields, add logic how to compute the new value
based on previous value.

```java
int catCountDiff = 4; // We want to increment by this amount

HashMap<String, String> userCounterNameMap = Maps.newHashMap();
HashMap<String, Object> userCounterValueMap = Maps.newHashMap();

userCounterNameMap.put("#catCount", "catCount");
userCounterValueMap.put(":diff", catCountDiff);
userCounterValueMap.put(":zero", 0L);

String upsertExpression = schema.upsertExpression(
        new CatCounter(bagId, catCountDiff),
        userCounterNameMap,
        userCounterValueMap,
        // Indicate we are computing catCount ourselves
        ImmutableSet.of("catCount"),
        // Compute catCount by adding existing value (or zero) to our catCountDiff
        ", #catCount = if_not_exists(#catCount, :zero) + :diff");
```

In this case, we have overwritten the `CatCounter` entirely except the `catCount` field. The field we are manually
calculating by adding the previous value if exists to our `catCountDiff`

### Filter records

One way to retrieve a subset of records is to replicate them into a secondary index conditionally based on the existence
of a field.

Currently there isn't a way to do this with this library, but would be fairly trivial to add, contributions are welcome.
