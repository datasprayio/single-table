// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.*;
import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import io.dataspray.singletable.DynamoConvertersProxy.*;
import io.dataspray.singletable.builder.*;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.GlobalSecondaryIndexProps;
import software.amazon.awscdk.services.dynamodb.LocalSecondaryIndexProps;
import software.amazon.awssdk.core.internal.waiters.DefaultWaiter;
import software.amazon.awssdk.core.waiters.Waiter;
import software.amazon.awssdk.core.waiters.WaiterAcceptor;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.internal.WaitersRuntime;
import software.constructs.Construct;

import javax.annotation.Nullable;
import java.lang.reflect.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.*;
import static io.dataspray.singletable.TableType.*;

@Slf4j
class DynamoMapperImpl implements DynamoMapper {
    private final String tableName;
    private final String indexPrefix;
    private final Gson gson;
    private final Converters converters;
    private final MarshallerAttrVal gsonMarshallerAttrVal;
    private final Function<Class, UnMarshallerAttrVal> gsonUnMarshallerAttrVal;
    @VisibleForTesting
    final Map<String, DynamoTable> rangePrefixToDynamoTable;

    DynamoMapperImpl(
            @Nullable String tableName,
            @Nullable String tablePrefix,
            Gson gson,
            @Nullable List<OverrideTypeConverter<?>> overrideTypeConverters,
            @Nullable List<OverrideCollectionTypeConverter<?>> overrideCollectionTypeConverters
    ) {
        this.tableName = tableName != null ? tableName : tablePrefix + Primary.name().toLowerCase();
        this.indexPrefix = tableName != null ? tableName : tablePrefix;
        this.gson = gson;
        this.converters = DynamoConvertersProxy.proxy(
                overrideTypeConverters == null ? List.of() : overrideTypeConverters,
                overrideCollectionTypeConverters == null ? List.of() : overrideCollectionTypeConverters);
        this.gsonMarshallerAttrVal = o -> AttributeValue.fromS(gson.toJson(o));
        this.gsonUnMarshallerAttrVal = k -> a -> gson.fromJson(a.s(), k);
        this.rangePrefixToDynamoTable = Maps.newHashMap();
    }

    @Override
    public String getTableName() {
        return getTableOrIndexName(Primary, -1);
    }

    @Override
    public software.amazon.awscdk.services.dynamodb.Table createCdkTable(Construct scope, String stackId, int lsiCount, int gsiCount) {
        software.amazon.awscdk.services.dynamodb.Table table = software.amazon.awscdk.services.dynamodb.Table.Builder.create(scope, stackId + "-singletable-table")
                .tableName(getTableName())
                .partitionKey(software.amazon.awscdk.services.dynamodb.Attribute.builder()
                        .name(getPartitionKeyName(Primary, -1)).type(AttributeType.STRING).build())
                .sortKey(software.amazon.awscdk.services.dynamodb.Attribute.builder()
                        .name(getRangeKeyName(Primary, -1)).type(AttributeType.STRING).build())
                .billingMode(software.amazon.awscdk.services.dynamodb.BillingMode.PAY_PER_REQUEST)
                .timeToLiveAttribute(SingleTable.TTL_IN_EPOCH_SEC_ATTR_NAME)
                .build();

        LongStream.range(1, lsiCount + 1).forEach(indexNumber -> {
            table.addLocalSecondaryIndex(LocalSecondaryIndexProps.builder()
                    .indexName(getTableOrIndexName(Lsi, indexNumber))
                    .projectionType(software.amazon.awscdk.services.dynamodb.ProjectionType.ALL)
                    .sortKey(software.amazon.awscdk.services.dynamodb.Attribute.builder()
                            .name(getRangeKeyName(Lsi, indexNumber))
                            .type(AttributeType.STRING).build())
                    .build());
        });

        LongStream.range(1, gsiCount + 1).forEach(indexNumber -> {
            table.addGlobalSecondaryIndex(GlobalSecondaryIndexProps.builder()
                    .indexName(getTableOrIndexName(Gsi, indexNumber))
                    .projectionType(software.amazon.awscdk.services.dynamodb.ProjectionType.ALL)
                    .partitionKey(software.amazon.awscdk.services.dynamodb.Attribute.builder()
                            .name(getPartitionKeyName(Gsi, indexNumber))
                            .type(AttributeType.STRING).build())
                    .sortKey(software.amazon.awscdk.services.dynamodb.Attribute.builder()
                            .name(getRangeKeyName(Gsi, indexNumber))
                            .type(AttributeType.STRING).build())
                    .build());
        });

        return table;
    }

    @Override
    public void createTableIfNotExists(DynamoDbClient dynamo, int lsiCount, int gsiCount) {
        String tableName = getTableName();
        TableDescription tableDescription;
        try {
            tableDescription = dynamo.describeTable(DescribeTableRequest.builder().tableName(tableName).build()).table();
        } catch (ResourceNotFoundException ex) {
            tableDescription = createTable(dynamo, lsiCount, gsiCount);
        }
        updateTableIndexes(dynamo, lsiCount, gsiCount, tableDescription);
        updateTableTtl(dynamo);
    }

    private TableDescription createTable(DynamoDbClient dynamo, int lsiCount, int gsiCount) {
        String tableName = getTableName();

        ArrayList<KeySchemaElement> primaryKeySchemas = Lists.newArrayList();
        ArrayList<AttributeDefinition> primaryAttributeDefinitions = Lists.newArrayList();
        ArrayList<LocalSecondaryIndex> localSecondaryIndexes = Lists.newArrayList();
        ArrayList<GlobalSecondaryIndex> globalSecondaryIndexes = Lists.newArrayList();

        primaryKeySchemas.add(KeySchemaElement.builder()
                .attributeName(getPartitionKeyName(Primary, -1))
                .keyType(KeyType.HASH).build());
        primaryAttributeDefinitions.add(AttributeDefinition.builder()
                .attributeName(getPartitionKeyName(Primary, -1))
                .attributeType(ScalarAttributeType.S).build());
        primaryKeySchemas.add(KeySchemaElement.builder()
                .attributeName(getRangeKeyName(Primary, -1))
                .keyType(KeyType.RANGE).build());
        primaryAttributeDefinitions.add(AttributeDefinition.builder()
                .attributeName(getRangeKeyName(Primary, -1))
                .attributeType(ScalarAttributeType.S)
                .build());

        LongStream.range(1, lsiCount + 1).forEach(indexNumber -> {
            localSecondaryIndexes.add(LocalSecondaryIndex.builder()
                    .indexName(getTableOrIndexName(Lsi, indexNumber))
                    .projection(Projection.builder()
                            .projectionType(ProjectionType.ALL).build())
                    .keySchema(ImmutableList.of(
                            KeySchemaElement.builder()
                                    .attributeName(getPartitionKeyName(Lsi, indexNumber))
                                    .keyType(KeyType.HASH).build(),
                            KeySchemaElement.builder()
                                    .attributeName(getRangeKeyName(Lsi, indexNumber))
                                    .keyType(KeyType.RANGE).build())).build());
            primaryAttributeDefinitions.add(AttributeDefinition.builder()
                    .attributeName(getRangeKeyName(Lsi, indexNumber))
                    .attributeType(ScalarAttributeType.S).build());
        });

        LongStream.range(1, gsiCount + 1).forEach(indexNumber -> {
            globalSecondaryIndexes.add(GlobalSecondaryIndex.builder()
                    .indexName(getTableOrIndexName(Gsi, indexNumber))
                    .projection(Projection.builder()
                            .projectionType(ProjectionType.ALL).build())
                    .keySchema(ImmutableList.of(
                            KeySchemaElement.builder()
                                    .attributeName(getPartitionKeyName(Gsi, indexNumber))
                                    .keyType(KeyType.HASH).build(),
                            KeySchemaElement.builder()
                                    .attributeName(getRangeKeyName(Gsi, indexNumber))
                                    .keyType(KeyType.RANGE).build())).build());
            primaryAttributeDefinitions.add(AttributeDefinition.builder()
                    .attributeName(getPartitionKeyName(Gsi, indexNumber))
                    .attributeType(ScalarAttributeType.S).build());
            primaryAttributeDefinitions.add(AttributeDefinition.builder()
                    .attributeName(getRangeKeyName(Gsi, indexNumber))
                    .attributeType(ScalarAttributeType.S).build());
        });

        CreateTableRequest.Builder createTableRequestBuilder = CreateTableRequest.builder()
                .tableName(tableName)
                .keySchema(primaryKeySchemas)
                .attributeDefinitions(primaryAttributeDefinitions)
                .billingMode(BillingMode.PAY_PER_REQUEST);
        if (!localSecondaryIndexes.isEmpty()) {
            createTableRequestBuilder.localSecondaryIndexes(localSecondaryIndexes);
        }
        if (!globalSecondaryIndexes.isEmpty()) {
            createTableRequestBuilder.globalSecondaryIndexes(globalSecondaryIndexes);
        }
        dynamo.createTable(createTableRequestBuilder.build());
        log.info("Table {} creating...", tableName);
        WaiterResponse<DescribeTableResponse> response = dynamo.waiter().waitUntilTableExists(DescribeTableRequest.builder()
                .tableName(tableName)
                .build());
        response.matched().exception().ifPresent(ex -> Throwables.propagate(ex));
        log.info("Table {} created", tableName);
        return response.matched().response().orElseThrow().table();
    }

    private void updateTableIndexes(DynamoDbClient dynamo, int lsiCount, int gsiCount, TableDescription tableDescription) {
        String tableName = getTableName();

        int lsiCountActual = tableDescription.localSecondaryIndexes().size();
        checkArgument(lsiCount == lsiCountActual, "Requested %s LSIs but table already has %s LSIs, LSIs cannot be changed without dropping the table.", lsiCount, lsiCountActual);

        Map<String, GlobalSecondaryIndexDescription> gsisToDelete = tableDescription.globalSecondaryIndexes().stream()
                .collect(Collectors.toMap(
                        GlobalSecondaryIndexDescription::indexName,
                        i -> i
                ));
        Set<Long> gsiIndexesToCreate = Sets.newHashSet();

        LongStream.range(1, gsiCount + 1).forEach(indexNumber -> {
            String gsiName = getTableOrIndexName(Gsi, indexNumber);
            if (gsisToDelete.remove(gsiName) == null) {
                gsiIndexesToCreate.add(indexNumber);
            }
        });
        if (gsiIndexesToCreate.isEmpty() && gsisToDelete.isEmpty()) {
            return;
        }
        Map<String, AttributeDefinition> primaryAttributeDefinitions = tableDescription.attributeDefinitions().stream()
                .collect(Collectors.toMap(
                        AttributeDefinition::attributeName,
                        i -> i));
        gsisToDelete.forEach((gsiNameToDelete, gsiToDelete) -> {
            log.info("Table {} deleting GSI index: {}", tableName, gsiNameToDelete);
            gsiToDelete.keySchema().stream()
                    .map(KeySchemaElement::attributeName)
                    .forEach(primaryAttributeDefinitions::remove);
            dynamo.updateTable(UpdateTableRequest.builder()
                    .tableName(tableName)
                    .attributeDefinitions(ImmutableList.copyOf(primaryAttributeDefinitions.values()))
                    .globalSecondaryIndexUpdates(GlobalSecondaryIndexUpdate.builder()
                            .delete(DeleteGlobalSecondaryIndexAction.builder()
                                    .indexName(gsiNameToDelete)
                                    .build()).build()).build());
            waitUntilGsiDeleted(dynamo, tableName, gsiNameToDelete);
        });
        gsiIndexesToCreate.forEach(indexNumber -> {
            String gsiNameToCreate = getTableOrIndexName(Gsi, indexNumber);
            log.info("Table {} creating GSI index: {}", tableName, gsiNameToCreate);
            primaryAttributeDefinitions.put(getPartitionKeyName(Gsi, indexNumber), AttributeDefinition.builder()
                    .attributeName(getPartitionKeyName(Gsi, indexNumber))
                    .attributeType(ScalarAttributeType.S).build());
            primaryAttributeDefinitions.put(getRangeKeyName(Gsi, indexNumber), AttributeDefinition.builder()
                    .attributeName(getRangeKeyName(Gsi, indexNumber))
                    .attributeType(ScalarAttributeType.S).build());
            dynamo.updateTable(UpdateTableRequest.builder()
                    .tableName(tableName)
                    .attributeDefinitions(ImmutableList.copyOf(primaryAttributeDefinitions.values()))
                    .globalSecondaryIndexUpdates(GlobalSecondaryIndexUpdate.builder()
                            .create(CreateGlobalSecondaryIndexAction.builder()
                                    .indexName(gsiNameToCreate)
                                    .projection(Projection.builder()
                                            .projectionType(ProjectionType.ALL).build())
                                    .keySchema(ImmutableList.of(
                                            KeySchemaElement.builder()
                                                    .attributeName(getPartitionKeyName(Gsi, indexNumber))
                                                    .keyType(KeyType.HASH).build(),
                                            KeySchemaElement.builder()
                                                    .attributeName(getRangeKeyName(Gsi, indexNumber))
                                                    .keyType(KeyType.RANGE).build()))
                                    .build()).build()).build());
            waitUntilGsiCreated(dynamo, tableName, gsiNameToCreate);
        });
    }

    private void updateTableTtl(DynamoDbClient dynamo) {
        String tableName = getTableName();

        TimeToLiveDescription desc = dynamo.describeTimeToLive(DescribeTimeToLiveRequest.builder()
                        .tableName(tableName)
                        .build())
                .timeToLiveDescription();
        boolean tableTtlExists = (TimeToLiveStatus.ENABLED.equals(desc.timeToLiveStatus())
                || TimeToLiveStatus.ENABLING.equals(desc.timeToLiveStatus()))
                && SingleTable.TTL_IN_EPOCH_SEC_ATTR_NAME.equals(desc.attributeName());
        if (!tableTtlExists) {
            dynamo.updateTimeToLive(UpdateTimeToLiveRequest.builder()
                    .tableName(tableName)
                    .timeToLiveSpecification(TimeToLiveSpecification.builder()
                            .enabled(true)
                            .attributeName(SingleTable.TTL_IN_EPOCH_SEC_ATTR_NAME).build()).build());
            log.info("Table {} Updated TTL", tableName);
        }
    }

    private WaiterResponse<DescribeTableResponse> waitUntilGsiCreated(DynamoDbClient dynamo, String tableName, String indexName) {
        Waiter.Builder<DescribeTableResponse> builder = DefaultWaiter.<DescribeTableResponse>builder()
                .addAcceptor(WaiterAcceptor.successOnResponseAcceptor(response -> getIndexStatus(response, indexName).map(IndexStatus.ACTIVE::equals).orElse(false)))
                .addAcceptor(WaiterAcceptor.retryOnResponseAcceptor(response -> getIndexStatus(response, indexName).map(IndexStatus.CREATING::equals).orElse(false)))
                .addAcceptor(WaiterAcceptor.errorOnResponseAcceptor(response -> getIndexStatus(response, indexName).map(IndexStatus.DELETING::equals).orElse(false)))
                .addAcceptor(WaiterAcceptor.errorOnResponseAcceptor(response -> getIndexStatus(response, indexName).isEmpty()));
        WaitersRuntime.DEFAULT_ACCEPTORS.forEach(builder::addAcceptor);
        return builder.build().run(() -> dynamo.describeTable(DescribeTableRequest.builder()
                .tableName(tableName)
                .build()));
    }

    private WaiterResponse<DescribeTableResponse> waitUntilGsiDeleted(DynamoDbClient dynamo, String tableName, String indexName) {
        Waiter.Builder<DescribeTableResponse> builder = DefaultWaiter.<DescribeTableResponse>builder()
                .addAcceptor(WaiterAcceptor.successOnResponseAcceptor(response -> getIndexStatus(response, indexName).isEmpty()))
                .addAcceptor(WaiterAcceptor.retryOnResponseAcceptor(response -> getIndexStatus(response, indexName).map(IndexStatus.DELETING::equals).orElse(false)))
                .addAcceptor(WaiterAcceptor.errorOnResponseAcceptor(response -> getIndexStatus(response, indexName).map(IndexStatus.CREATING::equals).orElse(false)))
                .addAcceptor(WaiterAcceptor.errorOnResponseAcceptor(response -> getIndexStatus(response, indexName).map(IndexStatus.ACTIVE::equals).orElse(false)));
        WaitersRuntime.DEFAULT_ACCEPTORS.forEach(builder::addAcceptor);
        return builder.build().run(() -> dynamo.describeTable(DescribeTableRequest.builder()
                .tableName(tableName)
                .build()));
    }

    private Optional<IndexStatus> getIndexStatus(DescribeTableResponse response, String indexName) {
        return response.table()
                .globalSecondaryIndexes()
                .stream()
                .filter(i -> indexName.equals(i.indexName()))
                .map(GlobalSecondaryIndexDescription::indexStatus)
                .findAny();
    }

    @Override
    public <T> TableSchema<T> parseTableSchema(Class<T> objClazz) {
        return parseSchema(Primary, -1, objClazz, false);
    }

    @Override
    public <T> IndexSchema<T> parseLocalSecondaryIndexSchema(long indexNumber, Class<T> objClazz) {
        return parseSchema(Lsi, indexNumber, objClazz, false);
    }

    @Override
    public <T> IndexSchema<T> parseGlobalSecondaryIndexSchema(long indexNumber, Class<T> objClazz) {
        return parseSchema(Gsi, indexNumber, objClazz, false);
    }

    @Override
    public <T> ShardedTableSchema<T> parseShardedTableSchema(Class<T> objClazz) {
        return parseSchema(Primary, -1, objClazz, true);
    }

    @Override
    public <T> ShardedIndexSchema<T> parseShardedLocalSecondaryIndexSchema(long indexNumber, Class<T> objClazz) {
        return parseSchema(Lsi, indexNumber, objClazz, true);
    }

    @Override
    public <T> ShardedIndexSchema<T> parseShardedGlobalSecondaryIndexSchema(long indexNumber, Class<T> objClazz) {
        return parseSchema(Gsi, indexNumber, objClazz, true);
    }

    private String getTableOrIndexName(TableType type, long indexNumber) {
        return type == Primary
                ? tableName
                : (indexPrefix + type.name().toLowerCase() + indexNumber);
    }

    private String getPartitionKeyName(TableType type, long indexNumber) {
        return type == Primary || type == Lsi
                ? "pk"
                : type.name().toLowerCase() + "pk" + indexNumber;
    }

    private String getRangeKeyName(TableType type, long indexNumber) {
        return type == Primary
                ? "sk"
                : type.name().toLowerCase() + "sk" + indexNumber;
    }

    public <T> String fieldMap(T obj, Field field) {
        try {
            return gson.toJson(checkNotNull(field.get(obj)));
        } catch (IllegalAccessException ex) {
            throw new RuntimeException(ex);
        }
    }

    public String mapMap(Map<String, Object> values, String partitionKey) {
        return gson.toJson(checkNotNull(values.get(partitionKey), "Partition key missing value for %s", partitionKey));
    }

    private <T> Function<T, String> getPartitionKeyValueObjGetter(Field[] partitionKeyFields, Field[] shardKeyFields, int shardCount, String shardPrefix) {
        return getPartitionKeyValueGetter(partitionKeyFields, shardKeyFields, shardCount, shardPrefix, this::fieldMap);
    }

    private Function<Map<String, Object>, String> getPartitionKeyValueMapGetter(String[] partitionKeys, String[] shardKeys, int shardCount, String shardPrefix) {
        return getPartitionKeyValueGetter(partitionKeys, shardKeys, shardCount, shardPrefix, this::mapMap);
    }

    private <T, F> Function<T, String> getPartitionKeyValueGetter(F[] partitionKeyFields, F[] shardKeyFields, int shardCount, String shardPrefix, BiFunction<T, F, String> fieldMapper) {
        BiFunction<T, Integer, String> partitionKeyValueGetter = getPartitionKeyValueGetter(partitionKeyFields, shardPrefix, fieldMapper);
        return shardKeyFields.length == 0
                ? obj -> partitionKeyValueGetter.apply(obj, null)
                : obj -> partitionKeyValueGetter.apply(obj,
                deterministicPartition(
                        StringSerdeUtil.mergeStrings(
                                Arrays.stream(shardKeyFields)
                                        .map(field -> fieldMapper.apply(obj, field))
                                        .toArray(String[]::new)), shardCount));
    }

    public int deterministicPartition(String input, int partitionCount) {
        return Math.abs(Hashing.murmur3_32_fixed().hashString(input, Charsets.UTF_8).asInt() % partitionCount);
    }

    private <T, F> BiFunction<T, Integer, String> getPartitionKeyValueGetter(F[] partitionKeyFields, String shardPrefix, BiFunction<T, F, String> fieldMapper) {
        return (obj, shard) -> StringSerdeUtil.mergeStrings(Stream.concat(
                        // First add all partition keys
                        Arrays.stream(partitionKeyFields)
                                .map(field -> fieldMapper.apply(obj, field)),
                        // Then add shard key last
                        shard == null ? Stream.of() : Stream.of(shardPrefix + "-" + shard))
                .toArray(String[]::new));
    }

    private <T> SchemaImpl<T> parseSchema(TableType type, long indexNumber, Class<T> objClazz, boolean expectToBeSharded) {
        DynamoTable[] dynamoTables = objClazz.getDeclaredAnnotationsByType(DynamoTable.class);
        checkState(dynamoTables.length > 0, "Class " + objClazz + " is missing DynamoTable annotation");
        DynamoTable dynamoTable = Arrays.stream(dynamoTables)
                .filter(dt -> dt.type() == type)
                .filter(dt -> dt.indexNumber() == indexNumber)
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Class " + objClazz + " is missing table type " + type));
        String[] partitionKeys = dynamoTable.partitionKeys();
        String[] shardKeys = dynamoTable.shardKeys();
        int shardCount = dynamoTable.shardCount();
        String shardPrefix = dynamoTable.shardPrefix();
        if (expectToBeSharded) {
            checkState(shardCount > -1, "Expecting shardCount for type " + type + " with index " + indexNumber);
            checkState(shardKeys.length > 0, "Expecting shardKeys for type " + type + " with index " + indexNumber);
        } else {
            checkState(shardCount == -1, "Not expecting shardCount for type " + type + " with index " + indexNumber);
            checkState("shard".equals(shardPrefix), "Not expecting shardPrefix for type " + type + " with index " + indexNumber);
            checkState(shardKeys.length == 0, "Not expecting shardKeys for type " + type + " with index " + indexNumber);
        }
        String[] rangeKeys = dynamoTable.rangeKeys();
        String rangePrefix = dynamoTable.rangePrefix();
        String tableName = getTableOrIndexName(Primary, indexNumber);
        Optional<String> indexNameOpt = type == Primary ? Optional.empty() : Optional.of(getTableOrIndexName(type, indexNumber));
        String partitionKeyName = getPartitionKeyName(type, indexNumber);
        String rangeKeyName = getRangeKeyName(type, indexNumber);

        DynamoTable dynamoTableOther = rangePrefixToDynamoTable.putIfAbsent(rangePrefix, dynamoTable);
        checkState(dynamoTableOther == null || dynamoTableOther == dynamoTable, "Detected multiple schemas with same rangePrefix %s, one in %s and other in %s", rangePrefix, dynamoTable, dynamoTableOther);

        ImmutableMap.Builder<String, MarshallerAttrVal> fieldAttrMarshallersBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, UnMarshallerAttrVal> fieldAttrUnMarshallersBuilder = ImmutableMap.builder();
        ImmutableList.Builder<Function<Map<String, AttributeValue>, Object>> fromAttrMapToCtorArgsListBuilder = ImmutableList.builder();
        ImmutableMap.Builder<String, Function<T, Object>> objToFieldValsBuilder = ImmutableMap.builder();
        Field[] partitionKeyFields = new Field[partitionKeys.length];
        Field[] shardKeyFields = new Field[shardKeys.length];
        Field[] rangeKeyFields = new Field[rangeKeys.length];
        ImmutableList.Builder<BiConsumer<ImmutableMap.Builder<String, AttributeValue>, T>> toAttrMapArgsBuilder = ImmutableList.builder();

        long fieldsCount = 0;
        for (Field field : objClazz.getDeclaredFields()) {
            if (field.isSynthetic()) {
                continue; // Skips fields such as "$jacocodata" during tests
            }
            fieldsCount++;
            String fieldName = field.getName();
            checkState(Modifier.isFinal(field.getModifiers()),
                    "Cannot map class %s to item,field %s is not final",
                    objClazz.getSimpleName(), fieldName);
            field.setAccessible(true);
            Optional<Class> collectionClazz = getCollectionClazz(field.getType());
            Class fieldClazz = collectionClazz.isPresent() ? getCollectionGeneric(field) : field.getType();

            // Sets and strings are special in that dynamo doesnt support
            // empty sets an empty strings so they require special handling
            boolean isSet = Set.class.isAssignableFrom(field.getType());
            boolean isString = String.class.isAssignableFrom(field.getType());

            boolean initWithDefault = field.isAnnotationPresent(InitWithDefault.class);
            Optional<DefaultInstanceGetter> defaultInstanceGetterOpt = !initWithDefault
                    ? Optional.empty() : findInClassSet(collectionClazz.orElse(fieldClazz), converters.di);
            if (initWithDefault && defaultInstanceGetterOpt.isEmpty()) {
                log.warn("Field {} with @NonNull missing default instance getter, please update DynamoConvertersProxy for class {}",
                        fieldName, collectionClazz.orElse(fieldClazz));
            }

            Function<T, Object> objToFieldVal = obj -> {
                Object o;
                try {
                    o = field.get(obj);
                } catch (IllegalAccessException ex) {
                    throw new RuntimeException(ex);
                }
                if (defaultInstanceGetterOpt.isPresent() && o == null) {
                    o = defaultInstanceGetterOpt.get().getDefaultInstance();
                }
                return o;
            };
            objToFieldValsBuilder.put(fieldName, objToFieldVal);

            // fromAttrMap
            UnMarshallerAttrVal unMarshallerAttrVal = findUnMarshallerAttrVal(collectionClazz, fieldClazz);
            fromAttrMapToCtorArgsListBuilder.add((attrMap) -> {
                AttributeValue attrVal = attrMap.get(fieldName);
                return (!isSet && (attrVal == null || attrVal.nul() == Boolean.TRUE))
                        ? defaultInstanceGetterOpt.map(DefaultInstanceGetter::getDefaultInstance).orElse(null)
                        : unMarshallerAttrVal.unmarshall(attrVal);
            });

            // toItem toAttrVal
            for (int i = 0; i < partitionKeys.length; i++) {
                if (fieldName.equals(partitionKeys[i])) {
                    partitionKeyFields[i] = field;
                }
            }
            for (int i = 0; i < shardKeys.length; i++) {
                if (fieldName.equals(shardKeys[i])) {
                    shardKeyFields[i] = field;
                }
            }
            for (int i = 0; i < rangeKeys.length; i++) {
                if (fieldName.equals(rangeKeys[i])) {
                    rangeKeyFields[i] = field;
                }
            }

            // toAttrVal
            MarshallerAttrVal marshallerAttrVal = findMarshallerAttrVal(collectionClazz, fieldClazz);
            toAttrMapArgsBuilder.add((mapBuilder, object) -> {
                Object val = objToFieldVal.apply(object);
                if (isSet && val == null) {
                    log.info("Field {} in class {} missing @NonNull. All sets are required to be non null since" +
                                    " empty set is not allowed by DynamoDB and there is no distinction between null and empty set.",
                            fieldName, object.getClass().getSimpleName());
                }
                if (isString && val != null && ((String) val).isEmpty()) {
                    log.info("Field {} in class {} set as empty string. All Strings are required to be either null or non empty since" +
                                    " empty string is not allowed by DynamoDB and there is no distinction between null and empty string.",
                            fieldName, object.getClass().getSimpleName());
                    val = null;
                }
                if (val == null) {
                    return; // Omit null
                }
                AttributeValue valMarsh = marshallerAttrVal.marshall(val);
                if (valMarsh == null) {
                    return; // Omit null
                }
                mapBuilder.put(fieldName, valMarsh);
            });

            // toAttrValue fromAttrValue
            fieldAttrMarshallersBuilder.put(fieldName, marshallerAttrVal);
            fieldAttrUnMarshallersBuilder.put(fieldName, unMarshallerAttrVal);
        }

        // fromItem fromAttrVal ctor
        Constructor<T> objCtor = findConstructor(objClazz, fieldsCount);
        objCtor.setAccessible(true);

        // fromAttrMap
        ImmutableList<Function<Map<String, AttributeValue>, Object>> fromAttrMapToCtorArgsList = fromAttrMapToCtorArgsListBuilder.build();
        Function<Map<String, AttributeValue>, Object[]> fromAttrMapToCtorArgs = attrMap -> fromAttrMapToCtorArgsList.stream()
                .map(u -> u.apply(attrMap))
                .toArray();

        // partitionKeyValueGetters
        Function<T, String> partitionKeyValueObjGetter = getPartitionKeyValueObjGetter(partitionKeyFields, shardKeyFields, shardCount, shardPrefix);
        Function<Map<String, Object>, String> partitionKeyValueMapGetter = getPartitionKeyValueMapGetter(partitionKeys, shardKeys, shardCount, shardPrefix);
        BiFunction<Map<String, Object>, Integer, String> partitionKeyValueMapShardGetter = getPartitionKeyValueGetter(partitionKeys, shardPrefix, this::mapMap);

        // toItem toAttrVal keys
        ImmutableMap<String, Function<T, Object>> objToFieldVals = objToFieldValsBuilder.build();
        ImmutableMap.Builder<String, Function<T, String>> toItemOtherKeysMapperBuilder = ImmutableMap.builder();
        for (DynamoTable dt : dynamoTables) {
            // This is a great place to sanitize validity of a DynamoTable
            // As we are iterating all the definitions given a class
            checkState((dt.partitionKeys().length + dt.shardKeys().length) > 0,
                    "Must supply partition keys and/or shard keys for class %s", objClazz);
            checkState(dt.shardKeys().length == 0 || !dt.shardPrefix().isEmpty(),
                    "Must supply shard prefix when using shard keys for class %s", objClazz);
            checkState(dt.shardKeys().length == 0 || dt.rangeKeys().length > 0,
                    "Must use range keys when using shard keys for class %s", objClazz);
            checkState(dt.shardKeys().length == 0 || dt.shardCount() > 0,
                    "Must supply shard count when using shard keys for class %s", objClazz);
            checkState(dt.shardKeys().length > 0 || dt.shardCount() == -1,
                    "Must leave shard count unset when not using shard keys for class %s", objClazz);
            checkState(!Strings.isNullOrEmpty(dt.rangePrefix()) || rangeKeys.length > 0,
                    "Must supply either list of range keys and/or a prefix for class %s", objClazz);
            String dtPartitionKeyName = getPartitionKeyName(dt.type(), dt.indexNumber());
            checkState(!objToFieldVals.containsKey(dtPartitionKeyName),
                    "Field name %s is reserved and cannot be used for class %s", dtPartitionKeyName, objClazz);
            String dtRangeKeyName = getRangeKeyName(dt.type(), dt.indexNumber());
            checkState(!objToFieldVals.containsKey(dtRangeKeyName),
                    "Field name %s is reserved and cannot be used for class %s", dtRangeKeyName, objClazz);
            if (dt.type() == dynamoTable.type() && dt.indexNumber() == dynamoTable.indexNumber()) {
                continue;
            }
            if (dt.type() != Lsi) {
                List<Function<T, String>> dtPartitionKeyMappers = Arrays.stream(dt.partitionKeys())
                        .map(objToFieldVals::get)
                        .map(Preconditions::checkNotNull)
                        .map(fun -> (Function<T, String>) (T obj) -> gson.toJson(fun.apply(obj)))
                        .collect(Collectors.toList());
                List<Function<T, String>> dtShardKeyMappers = Arrays.stream(dt.shardKeys())
                        .map(objToFieldVals::get)
                        .map(Preconditions::checkNotNull)
                        .map(fun -> (Function<T, String>) (T obj) -> gson.toJson(fun.apply(obj)))
                        .collect(Collectors.toList());
                Function<T, String> partitionKeyValueGetter = this.<T, Function<T, String>>getPartitionKeyValueGetter(
                        dtPartitionKeyMappers.toArray(Function[]::new),
                        dtShardKeyMappers.toArray(Function[]::new),
                        dt.shardCount(),
                        dt.shardPrefix(),
                        (obj, mapper) -> mapper.apply(obj));
                toItemOtherKeysMapperBuilder.put(
                        dtPartitionKeyName,
                        partitionKeyValueGetter);
            }
            String dtRangePrefix = dt.rangePrefix();
            ImmutableList<Function<T, Object>> dtRangeKeyMappers = Arrays.stream(dt.rangeKeys())
                    .map(key -> checkNotNull(objToFieldVals.get(key), "Field does not exist: %s", key))
                    .collect(ImmutableList.toImmutableList());
            toItemOtherKeysMapperBuilder.put(
                    dtRangeKeyName,
                    obj -> StringSerdeUtil.mergeStrings(Stream.concat(Stream.of(dtRangePrefix), dtRangeKeyMappers.stream()
                                    .map(m -> m.apply(obj))
                                    .map(gson::toJson))
                            .toArray(String[]::new)));
        }
        ImmutableMap<String, Function<T, String>> toItemOtherKeysMapper = toItemOtherKeysMapperBuilder.build();
        Function<T, String> getPartitionKeyVal = partitionKeyValueObjGetter;
        Function<T, String> getRangeKeyVal = obj -> StringSerdeUtil.mergeStrings(Stream.concat(Stream.of(rangePrefix), Arrays.stream(rangeKeyFields)
                        .map(field -> fieldMap(obj, field)))
                .toArray(String[]::new));

        // toAttrMap
        ImmutableList<BiConsumer<ImmutableMap.Builder<String, AttributeValue>, T>> toAttrMapArgs = toAttrMapArgsBuilder.build();
        Function<T, ImmutableMap<String, AttributeValue>> toAttrMapMapper = obj -> {
            ImmutableMap.Builder<String, AttributeValue> attrMapBuilder = ImmutableMap.builder();
            attrMapBuilder.put(partitionKeyName, AttributeValue.fromS(getPartitionKeyVal.apply(obj)));
            attrMapBuilder.put(rangeKeyName, AttributeValue.fromS(getRangeKeyVal.apply(obj)));
            toItemOtherKeysMapper.forEach(((keyName, objToKeyMapper) ->
                    attrMapBuilder.put(keyName, AttributeValue.fromS(objToKeyMapper.apply(obj)))));
            toAttrMapArgs.forEach(m -> m.accept(attrMapBuilder, obj));
            return attrMapBuilder.build();
        };

        // toAttrValue fromAttrValue
        ImmutableMap<String, MarshallerAttrVal> fieldAttrMarshallers = fieldAttrMarshallersBuilder.build();
        ImmutableMap<String, UnMarshallerAttrVal> fieldAttrUnMarshallers = fieldAttrUnMarshallersBuilder.build();

        return new SchemaImpl<T>(
                type,
                partitionKeys,
                shardKeys,
                shardCount,
                rangeKeys,
                partitionKeyFields,
                rangeKeyFields,
                rangePrefix,
                tableName,
                indexNameOpt,
                partitionKeyName,
                rangeKeyName,
                fieldAttrMarshallers,
                fieldAttrUnMarshallers,
                fromAttrMapToCtorArgs,
                objCtor,
                toAttrMapMapper,
                partitionKeyValueObjGetter,
                partitionKeyValueMapGetter,
                partitionKeyValueMapShardGetter);
    }

    private <T> Constructor<T> findConstructor(Class<T> objectClazz, long argc) {
        for (Constructor<?> constructorPotential : objectClazz.getDeclaredConstructors()) {
            // Let's only check for args size and assume all types are good...
            if (constructorPotential.getParameterCount() != argc) {
                log.trace("Unsuitable constructor {}", constructorPotential);
                continue;
            }
            return (Constructor<T>) constructorPotential;
        }
        throw new IllegalStateException("Cannot find constructor for class " + objectClazz.getSimpleName());
    }

    private Optional<Class> getCollectionClazz(Class<?> clazz) {
        return Collection.class.isAssignableFrom(clazz) || Map.class.isAssignableFrom(clazz)
                ? Optional.of(clazz)
                : Optional.empty();
    }

    private Class getCollectionGeneric(Parameter parameter) {
        if (Map.class.isAssignableFrom(parameter.getType())) {
            return ((Class) ((ParameterizedType) parameter.getParameterizedType())
                    .getActualTypeArguments()[1]);
        } else {
            return ((Class) ((ParameterizedType) parameter.getParameterizedType())
                    .getActualTypeArguments()[0]);
        }
    }

    private Class getCollectionGeneric(Field field) {
        if (Map.class.isAssignableFrom(field.getType())) {
            return ((Class) ((ParameterizedType) field.getGenericType())
                    .getActualTypeArguments()[1]);
        } else {
            return ((Class) ((ParameterizedType) field.getGenericType())
                    .getActualTypeArguments()[0]);
        }
    }

    private Class getCollectionGeneric(Object object) {
        if (Map.class.isAssignableFrom(object.getClass())) {
            Map map = (Map) object;
            if (map.isEmpty()) {
                return Object.class;
            } else {
                return map.values().iterator().next().getClass();
            }
        } else {
            Collection collection = (Collection) object;
            if (collection.isEmpty()) {
                return Object.class;
            } else {
                return collection.iterator().next().getClass();
            }
        }
    }

    private MarshallerAttrVal findMarshallerAttrVal(Optional<Class> collectionClazz, Class itemClazz) {
        MarshallerAttrVal f = findInClassSet(itemClazz, converters.mp).orElse(gsonMarshallerAttrVal);
        if (collectionClazz.isPresent()) {
            CollectionMarshallerAttrVal fc = findInClassSet(collectionClazz.get(), converters.mc).get();
            return o -> fc.marshall(o, f);
        } else {
            return f;
        }
    }

    private UnMarshallerAttrVal findUnMarshallerAttrVal(Optional<Class> collectionClazz, Class itemClazz) {
        UnMarshallerAttrVal f = findInClassSet(itemClazz, converters.up).orElseGet(() -> gsonUnMarshallerAttrVal.apply(itemClazz));
        if (collectionClazz.isPresent()) {
            CollectionUnMarshallerAttrVal fc = findInClassSet(collectionClazz.get(), converters.uc).get();
            return a -> fc.unmarshall(a, f);
        } else {
            return f;
        }
    }

    private <T> Optional<T> findInClassSet(Class clazz, ImmutableSet<Entry<Class<?>, T>> set) {
        for (Entry<Class<?>, T> entry : set) {
            if (entry.getKey().isAssignableFrom(clazz)) {
                return Optional.of(entry.getValue());
            }
        }
        return Optional.empty();
    }

    @RequiredArgsConstructor
    public class SchemaImpl<T> implements TableSchema<T>, ShardedTableSchema<T>, IndexSchema<T>, ShardedIndexSchema<T> {
        private final TableType type;
        private final String[] partitionKeys;
        private final String[] shardKeys;
        private final int shardCount;
        private final String[] rangeKeys;
        private final Field[] partitionKeyFields;
        private final Field[] rangeKeyFields;
        private final String rangePrefix;
        private final String tableName;
        private final Optional<String> indexNameOpt;
        private final String partitionKeyName;
        private final String rangeKeyName;
        private final ImmutableMap<String, MarshallerAttrVal> fieldAttrMarshallers;
        private final ImmutableMap<String, UnMarshallerAttrVal> fieldAttrUnMarshallers;
        private final Function<Map<String, AttributeValue>, Object[]> fromAttrMapToCtorArgs;
        private final Constructor<T> objCtor;
        private final Function<T, ImmutableMap<String, AttributeValue>> toAttrMapMapper;
        private final Function<T, String> partitionKeyValueObjGetter;
        private final Function<Map<String, Object>, String> partitionKeyValueMapGetter;
        private final BiFunction<Map<String, Object>, Integer, String> partitionKeyValueMapShardGetter;

        @Override
        public String tableName() {
            return tableName;
        }

        @Override
        public QueryBuilder<T> query() {
            return new QueryBuilder<>(this, indexNameOpt);
        }

        @Override
        public ShardedQueryBuilder<T> querySharded() {
            return new ShardedQueryBuilder<>(this, indexNameOpt);
        }

        @Override
        public GetBuilder<T> get() {
            return new GetBuilder<>(this);
        }

        @Override
        public PutBuilder<T> put() {
            return new PutBuilder<>(this);
        }

        @Override
        public DeleteBuilder<T> delete() {
            return new DeleteBuilder<>(this);
        }

        @Override
        public UpdateBuilder<T> update() {
            return new UpdateBuilder<>(this);
        }

        public Optional<String> indexNameOpt() {
            return indexNameOpt;
        }

        @Override
        public String indexName() {
            return indexNameOpt.orElseThrow();
        }

        @Override
        public Map<String, AttributeValue> primaryKey(T obj) {
            return Map.ofEntries(
                    partitionKey(obj),
                    rangeKey(obj));
        }

        @Override
        public Map<String, AttributeValue> primaryKey(Map<String, Object> values) {
            checkState(partitionKeys.length + rangeKeys.length + shardKeys.length >= values.size(), "Unexpected extra values, partition keys %s range keys %s shard keys %s values %s",
                    Arrays.toString(partitionKeys), Arrays.toString(rangeKeys), Arrays.toString(shardKeys), values);
            return Map.ofEntries(
                    partitionKey(values),
                    rangeKey(values, false));
        }

        @Override
        public String partitionKeyName() {
            return partitionKeyName;
        }

        @Override
        public Entry<String, AttributeValue> partitionKey(T obj) {
            return Maps.immutableEntry(partitionKeyName, AttributeValue.builder()
                    .s(partitionKeyValue(obj)).build());
        }

        @Override
        public Entry<String, AttributeValue> partitionKey(Map<String, Object> values) {
            return Maps.immutableEntry(partitionKeyName, AttributeValue.builder()
                    .s(partitionKeyValue(values)).build());
        }

        @Override
        public Map<String, Condition> attrMapToConditions(Entry<String, AttributeValue> attrEntry) {
            return attrMapToConditions(Map.ofEntries(attrEntry));
        }

        @Override
        public Map<String, Condition> attrMapToConditions(Map<String, AttributeValue> attrMap) {
            return Maps.transformValues(attrMap, attrVal -> Condition.builder()
                    .comparisonOperator(ComparisonOperator.EQ)
                    .attributeValueList(attrVal)
                    .build());
        }

        @Override
        public Entry<String, AttributeValue> shardKey(int shard) {
            checkArgument(partitionKeys.length > 0, "Partition keys are required, call shardKey(shard, values) instead");
            return shardKey(shard, Map.of());
        }

        @Override
        public Entry<String, AttributeValue> shardKey(int shard, Map<String, Object> values) {
            checkArgument(shardKeys.length > 0, "Cannot construct a shard key for schema with no shardKeys defined");
            checkArgument(shard >= 0, "Shard number " + shard + " cannot be negative");
            checkArgument(shard < shardCount, "Shard number starts with zero and must be less than the maximum shard count of " + shardCount);
            return Maps.immutableEntry(
                    partitionKeyName,
                    AttributeValue.fromS(partitionKeyValueMapShardGetter.apply(values, shard)));
        }

        @Override
        public String partitionKeyValue(T obj) {
            return partitionKeyValueObjGetter.apply(obj);
        }

        @Override
        public String partitionKeyValue(Map<String, Object> values) {
            return partitionKeyValueMapGetter.apply(values);
        }

        @Override
        public String rangeKeyName() {
            return rangeKeyName;
        }

        @Override
        public Entry<String, AttributeValue> rangeKey(T obj) {
            return Maps.immutableEntry(
                    rangeKeyName,
                    AttributeValue.fromS(StringSerdeUtil.mergeStrings(Stream.concat(Stream.of(rangePrefix), Arrays.stream(rangeKeyFields)
                                    .map(rangeKeyField -> {
                                        try {
                                            return gson.toJson(checkNotNull(rangeKeyField.get(obj),
                                                    "Range key value null, should add @NonNull on all keys for class %s", obj));
                                        } catch (IllegalAccessException ex) {
                                            throw new RuntimeException(ex);
                                        }
                                    }))
                            .toArray(String[]::new))));
        }

        @Override
        public Entry<String, AttributeValue> rangeKey(Map<String, Object> values) {
            checkState(rangeKeys.length == values.size(), "Unexpected extra values, range keys %s values %s", rangeKeys, values);
            return rangeKey(values, true);
        }

        private Entry<String, AttributeValue> rangeKey(Map<String, Object> values, boolean check) {
            checkState(!check || rangeKeys.length == values.size(), "Unexpected extra values, range keys %s values %s", rangeKeys, values);
            return Maps.immutableEntry(
                    rangeKeyName,
                    AttributeValue.fromS(StringSerdeUtil.mergeStrings(Stream.concat(Stream.of(rangePrefix), Arrays.stream(rangeKeys)
                                    .map(rangeKey -> gson.toJson(checkNotNull(values.get(rangeKey), "Range key missing value for %s", rangeKey))))
                            .toArray(String[]::new))));
        }

        @Override
        public Entry<String, AttributeValue> rangeKeyPartial(Map<String, Object> values) {
            return Maps.immutableEntry(
                    rangeKeyName,
                    AttributeValue.fromS(rangeValuePartial(values)));
        }

        @Override
        public String rangeValuePartial(Map<String, Object> values) {
            return StringSerdeUtil.mergeStrings(Stream.concat(Stream.of(rangePrefix), Arrays.stream(rangeKeys)
                            .map(values::get)
                            .takeWhile(Objects::nonNull)
                            .map(gson::toJson))
                    .toArray(String[]::new));
        }

        @Override
        public AttributeValue toAttrValue(Object object) {
            if (object instanceof AttributeValue) {
                return (AttributeValue) object;
            }
            Optional<Class> collectionClazz = getCollectionClazz(object.getClass());
            Class fieldClazz = collectionClazz.isPresent() ? getCollectionGeneric(object) : object.getClass();
            MarshallerAttrVal marshaller = findMarshallerAttrVal(collectionClazz, fieldClazz);
            if (marshaller == null) {
                throw new RuntimeException("Cannot find marshaller for " + object.getClass());
            }
            return marshaller.marshall(object);
        }

        @Override
        public AttributeValue toAttrValue(String fieldName, Object object) {
            MarshallerAttrVal marshaller = fieldAttrMarshallers.get(fieldName);
            if (marshaller == null) {
                throw new RuntimeException("Cannot find marshaller for field " + fieldName);
            }
            return marshaller.marshall(object);
        }

        @Override
        public Object fromAttrValue(String fieldName, AttributeValue attrVal) {
            UnMarshallerAttrVal unMarshaller = fieldAttrUnMarshallers.get(fieldName);
            if (unMarshaller == null) {
                throw new RuntimeException("Cannot find unmarshaller for field " + fieldName);
            }
            return unMarshaller.unmarshall(attrVal);
        }

        @Override
        public ImmutableMap<String, AttributeValue> toAttrMap(T object) {
            if (object == null) {
                return null;
            }
            return toAttrMapMapper.apply(object);
        }

        @Override
        public T fromAttrMap(Map<String, AttributeValue> attrMap) {
            if (attrMap == null || attrMap.isEmpty()) {
                return null;
            }
            try {
                return objCtor.newInstance(fromAttrMapToCtorArgs.apply(attrMap));
            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException |
                     InvocationTargetException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public int shardCount() {
            return shardCount;
        }

        @Override
        public Optional<String> serializeLastEvaluatedKey(Map<String, AttributeValue> lastEvaluatedKey) {
            if (lastEvaluatedKey.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(gson.toJson(Maps.transformValues(lastEvaluatedKey, AttributeValue::s)));
        }

        @Override
        public Map<String, AttributeValue> toExclusiveStartKey(String serializedlastEvaluatedKey) {
            Map<String, String> attributes = gson.fromJson(serializedlastEvaluatedKey, new TypeToken<Map<String, String>>() {
            }.getType());
            return toExclusiveStartKey(attributes);
        }

        private Map<String, AttributeValue> toExclusiveStartKey(Map<String, String> attributes) {
            return Maps.transformValues(attributes, AttributeValue::fromS);
        }

        @Override
        public String serializeShardedLastEvaluatedKey(Optional<Map<String, AttributeValue>> lastEvaluatedKeyOpt, int shard) {
            return gson.toJson(new ShardAndAttributes(
                    shard,
                    lastEvaluatedKeyOpt
                            .map(lastEvaluatedKey -> Maps.transformValues(lastEvaluatedKey, AttributeValue::s))
                            .orElse(null)));
        }

        @Override
        public ShardAndExclusiveStartKey wrapShardedLastEvaluatedKey(Optional<Map<String, AttributeValue>> lastEvaluatedKeyOpt, int shard) {
            return new ShardAndExclusiveStartKey(
                    shard,
                    lastEvaluatedKeyOpt);
        }

        @Override
        public String serializeShardedLastEvaluatedKey(ShardAndExclusiveStartKey shardAndExclusiveStartKey) {
            return gson.toJson(new ShardAndAttributes(
                    shardAndExclusiveStartKey.getShard(),
                    shardAndExclusiveStartKey.getExclusiveStartKey()
                            .map(primaryKey -> Maps.transformValues(primaryKey, AttributeValue::s))
                            .orElse(null)));
        }

        @Override
        public ShardAndExclusiveStartKey toShardedExclusiveStartKey(String serializedShardedLastEvaluatedKey) {
            ShardAndAttributes shardAndAttributes = gson.fromJson(serializedShardedLastEvaluatedKey, ShardAndAttributes.class);
            return new ShardAndExclusiveStartKey(
                    shardAndAttributes.getShard(),
                    Optional.ofNullable(shardAndAttributes.getPrimaryKeyAttributes()).map(this::toExclusiveStartKey));
        }
    }

    @Value
    private static class ShardAndAttributes {
        @NonNull
        @SerializedName("s")
        int shard;

        @SerializedName("d")
        Map<String, String> primaryKeyAttributes;
    }
}
