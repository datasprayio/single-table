// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.api.QueryApi;
import com.amazonaws.services.dynamodbv2.document.api.ScanApi;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.*;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import io.dataspray.singletable.DynamoConvertersProxy.*;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.GlobalSecondaryIndexProps;
import software.amazon.awscdk.services.dynamodb.LocalSecondaryIndexProps;
import software.constructs.Construct;

import java.lang.reflect.*;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.*;
import static io.dataspray.singletable.TableType.*;

@Slf4j
class DynamoMapperImpl implements DynamoMapper {
    private final String tablePrefix;
    private final Gson gson;
    private final AmazonDynamoDB dynamo;
    private final DynamoDB dynamoDoc;
    private final Converters converters;
    private final MarshallerItem gsonMarshallerItem;
    private final MarshallerAttrVal gsonMarshallerAttrVal;
    private final Function<Class, UnMarshallerAttrVal> gsonUnMarshallerAttrVal;
    private final Function<Class, UnMarshallerItem> gsonUnMarshallerItem;
    @VisibleForTesting
    final Map<String, DynamoTable> rangePrefixToDynamoTable;

    DynamoMapperImpl(String tablePrefix, Gson gson, AmazonDynamoDB dynamo, DynamoDB dynamoDoc) {
        this.tablePrefix = tablePrefix;
        this.gson = gson;
        this.dynamo = dynamo;
        this.dynamoDoc = dynamoDoc;
        this.converters = DynamoConvertersProxy.proxy();
        this.gsonMarshallerItem = (o, a, i) -> i.withString(a, gson.toJson(o));
        this.gsonMarshallerAttrVal = o -> new AttributeValue().withS(gson.toJson(o));
        this.gsonUnMarshallerAttrVal = k -> a -> gson.fromJson(a.getS(), k);
        this.gsonUnMarshallerItem = k -> (a, i) -> gson.fromJson(i.getString(a), k);
        this.rangePrefixToDynamoTable = Maps.newHashMap();
    }

    @Override
    public software.amazon.awscdk.services.dynamodb.Table createCdkTable(Construct scope, String stackId, int lsiCount, int gsiCount) {
        software.amazon.awscdk.services.dynamodb.Table table = software.amazon.awscdk.services.dynamodb.Table.Builder.create(scope, stackId + "-singletable-table")
                .tableName(getTableOrIndexName(Primary, -1))
                .partitionKey(software.amazon.awscdk.services.dynamodb.Attribute.builder()
                        .name(getPartitionKeyName(Primary, -1)).type(AttributeType.STRING).build())
                .sortKey(software.amazon.awscdk.services.dynamodb.Attribute.builder()
                        .name(getRangeKeyName(Primary, -1)).type(AttributeType.STRING).build())
                .billingMode(software.amazon.awscdk.services.dynamodb.BillingMode.PAY_PER_REQUEST)
                .timeToLiveAttribute(SingleTable.TTL_IN_EPOCH_SEC_ATTR_NAME)
                .build();

        ImmutableList.builder();
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
    public void createTableIfNotExists(int lsiCount, int gsiCount) {
        String tableName = getTableOrIndexName(Primary, -1);
        boolean tableExists;
        boolean tableTtlExists;
        try {
            DescribeTimeToLiveResult timeToLiveResult = dynamo.describeTimeToLive(new DescribeTimeToLiveRequest()
                    .withTableName(tableName));
            tableExists = true;
            tableTtlExists = (TimeToLiveStatus.ENABLED.name().equals(timeToLiveResult.getTimeToLiveDescription().getTimeToLiveStatus())
                    || TimeToLiveStatus.ENABLING.name().equals(timeToLiveResult.getTimeToLiveDescription().getTimeToLiveStatus()))
                    && SingleTable.TTL_IN_EPOCH_SEC_ATTR_NAME.equals(timeToLiveResult.getTimeToLiveDescription().getAttributeName());
        } catch (ResourceNotFoundException ex) {
            tableExists = false;
            tableTtlExists = false;
        }
        if (!tableExists) {
            ArrayList<KeySchemaElement> primaryKeySchemas = Lists.newArrayList();
            ArrayList<AttributeDefinition> primaryAttributeDefinitions = Lists.newArrayList();
            ArrayList<LocalSecondaryIndex> localSecondaryIndexes = Lists.newArrayList();
            ArrayList<GlobalSecondaryIndex> globalSecondaryIndexes = Lists.newArrayList();

            primaryKeySchemas.add(new KeySchemaElement(getPartitionKeyName(Primary, -1), KeyType.HASH));
            primaryAttributeDefinitions.add(new AttributeDefinition(getPartitionKeyName(Primary, -1), ScalarAttributeType.S));
            primaryKeySchemas.add(new KeySchemaElement(getRangeKeyName(Primary, -1), KeyType.RANGE));
            primaryAttributeDefinitions.add(new AttributeDefinition(getRangeKeyName(Primary, -1), ScalarAttributeType.S));

            LongStream.range(1, lsiCount + 1).forEach(indexNumber -> {
                localSecondaryIndexes.add(new LocalSecondaryIndex()
                        .withIndexName(getTableOrIndexName(Lsi, indexNumber))
                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
                        .withKeySchema(ImmutableList.of(
                                new KeySchemaElement(getPartitionKeyName(Lsi, indexNumber), KeyType.HASH),
                                new KeySchemaElement(getRangeKeyName(Lsi, indexNumber), KeyType.RANGE))));
                primaryAttributeDefinitions.add(new AttributeDefinition(getRangeKeyName(Lsi, indexNumber), ScalarAttributeType.S));
            });

            LongStream.range(1, gsiCount + 1).forEach(indexNumber -> {
                globalSecondaryIndexes.add(new GlobalSecondaryIndex()
                        .withIndexName(getTableOrIndexName(Gsi, indexNumber))
                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
                        .withKeySchema(ImmutableList.of(
                                new KeySchemaElement(getPartitionKeyName(Gsi, indexNumber), KeyType.HASH),
                                new KeySchemaElement(getRangeKeyName(Gsi, indexNumber), KeyType.RANGE))));
                primaryAttributeDefinitions.add(new AttributeDefinition(getPartitionKeyName(Gsi, indexNumber), ScalarAttributeType.S));
                primaryAttributeDefinitions.add(new AttributeDefinition(getRangeKeyName(Gsi, indexNumber), ScalarAttributeType.S));
            });

            CreateTableRequest createTableRequest = new CreateTableRequest()
                    .withTableName(tableName)
                    .withKeySchema(primaryKeySchemas)
                    .withAttributeDefinitions(primaryAttributeDefinitions)
                    .withBillingMode(BillingMode.PAY_PER_REQUEST);
            if (!localSecondaryIndexes.isEmpty()) {
                createTableRequest.withLocalSecondaryIndexes(localSecondaryIndexes);
            }
            if (!globalSecondaryIndexes.isEmpty()) {
                createTableRequest.withGlobalSecondaryIndexes(globalSecondaryIndexes);
            }
            dynamoDoc.createTable(createTableRequest);
            log.info("Table {} created", tableName);
        }
        if (!tableTtlExists) {
            dynamo.updateTimeToLive(new UpdateTimeToLiveRequest()
                    .withTableName(tableName)
                    .withTimeToLiveSpecification(new TimeToLiveSpecification()
                            .withEnabled(true)
                            .withAttributeName(SingleTable.TTL_IN_EPOCH_SEC_ATTR_NAME)));
        }
    }

    @Override
    public <T> TableSchema<T> parseTableSchema(Class<T> objClazz) {
        return parseSchema(Primary, -1, objClazz);
    }

    @Override
    public <T> IndexSchema<T> parseLocalSecondaryIndexSchema(long indexNumber, Class<T> objClazz) {
        return parseSchema(Lsi, indexNumber, objClazz);
    }

    @Override
    public <T> IndexSchema<T> parseGlobalSecondaryIndexSchema(long indexNumber, Class<T> objClazz) {
        return parseSchema(Gsi, indexNumber, objClazz);
    }

    private String getTableOrIndexName(TableType type, long indexNumber) {
        return tablePrefix + (type == Primary
                ? type.name().toLowerCase()
                : type.name().toLowerCase() + indexNumber);
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
                DynamoUtil.deterministicPartition(
                        StringSerdeUtil.mergeStrings(
                                Arrays.stream(shardKeyFields)
                                        .map(field -> fieldMapper.apply(obj, field))
                                        .toArray(String[]::new)), shardCount));
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

    private <T> SchemaImpl<T> parseSchema(TableType type, long indexNumber, Class<T> objClazz) {
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
        String[] rangeKeys = dynamoTable.rangeKeys();
        String rangePrefix = dynamoTable.rangePrefix();
        String tableName = getTableOrIndexName(type, indexNumber);
        String partitionKeyName = getPartitionKeyName(type, indexNumber);
        String rangeKeyName = getRangeKeyName(type, indexNumber);

        DynamoTable dynamoTableOther = rangePrefixToDynamoTable.putIfAbsent(rangePrefix, dynamoTable);
        checkState(dynamoTableOther == null || dynamoTableOther == dynamoTable, "Detected multiple schemas with same rangePrefix %s, one in %s and other in %s", rangePrefix, dynamoTable, dynamoTableOther);

        Table table = dynamoDoc.getTable(getTableOrIndexName(Primary, -1));
        Index index = type != Primary
                ? table.getIndex(tableName)
                : null;

        ImmutableMap.Builder<String, MarshallerItem> fieldMarshallersBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, UnMarshallerItem> fieldUnMarshallersBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, MarshallerAttrVal> fieldAttrMarshallersBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, UnMarshallerAttrVal> fieldAttrUnMarshallersBuilder = ImmutableMap.builder();
        ImmutableList.Builder<Function<Item, Object>> fromItemToCtorArgsListBuilder = ImmutableList.builder();
        ImmutableList.Builder<Function<Map<String, AttributeValue>, Object>> fromAttrMapToCtorArgsListBuilder = ImmutableList.builder();
        ImmutableMap.Builder<String, Function<T, Object>> objToFieldValsBuilder = ImmutableMap.builder();
        Field[] partitionKeyFields = new Field[partitionKeys.length];
        Field[] shardKeyFields = new Field[shardKeys.length];
        Field[] rangeKeyFields = new Field[rangeKeys.length];
        ImmutableList.Builder<BiConsumer<Item, T>> toItemArgsBuilder = ImmutableList.builder();
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

            // fromItem
            UnMarshallerItem unMarshallerItem = findUnMarshallerItem(collectionClazz, fieldClazz);
            fromItemToCtorArgsListBuilder.add((item) ->
                    (!isSet && (!item.isPresent(fieldName) || item.isNull(fieldName)))
                            ? defaultInstanceGetterOpt.map(DefaultInstanceGetter::getDefaultInstance).orElse(null)
                            : unMarshallerItem.unmarshall(fieldName, item));

            // fromAttrMap
            UnMarshallerAttrVal unMarshallerAttrVal = findUnMarshallerAttrVal(collectionClazz, fieldClazz);
            fromAttrMapToCtorArgsListBuilder.add((attrMap) -> {
                AttributeValue attrVal = attrMap.get(fieldName);
                return (!isSet && (attrVal == null || attrVal.getNULL() == Boolean.TRUE))
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

            // toItem
            MarshallerItem marshallerItem = findMarshallerItem(collectionClazz, fieldClazz);
            toItemArgsBuilder.add((item, object) -> {
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
                marshallerItem.marshall(val, fieldName, item);
            });

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

            // toDynamoValue fromDynamoValue
            fieldMarshallersBuilder.put(fieldName, marshallerItem);
            fieldUnMarshallersBuilder.put(fieldName, unMarshallerItem);

            // toAttrValue fromAttrValue
            fieldAttrMarshallersBuilder.put(fieldName, marshallerAttrVal);
            fieldAttrUnMarshallersBuilder.put(fieldName, unMarshallerAttrVal);
        }

        // fromItem fromAttrVal ctor
        Constructor<T> objCtor = findConstructor(objClazz, fieldsCount);
        objCtor.setAccessible(true);

        // fromItem
        ImmutableList<Function<Item, Object>> fromItemToCtorArgsList = fromItemToCtorArgsListBuilder.build();
        Function<Item, Object[]> fromItemToCtorArgs = item -> fromItemToCtorArgsList.stream()
                .map(u -> u.apply(item))
                .toArray();

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
                        shardCount,
                        shardPrefix,
                        (obj, mapper) -> mapper.apply(obj));
                toItemOtherKeysMapperBuilder.put(
                        getPartitionKeyName(dt.type(), dt.indexNumber()),
                        partitionKeyValueGetter);
            }
            String dtRangePrefix = dt.rangePrefix();
            ImmutableList<Function<T, Object>> dtRangeKeyMappers = Arrays.stream(dt.rangeKeys())
                    .map(objToFieldVals::get)
                    .map(Preconditions::checkNotNull)
                    .collect(ImmutableList.toImmutableList());
            toItemOtherKeysMapperBuilder.put(
                    getRangeKeyName(dt.type(), dt.indexNumber()),
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

        // toItem
        ImmutableList<BiConsumer<Item, T>> toItemArgs = toItemArgsBuilder.build();
        Function<T, Item> toItemMapper = obj -> {
            Item item = new Item();
            item.withPrimaryKey(partitionKeyName, getPartitionKeyVal.apply(obj),
                    rangeKeyName, getRangeKeyVal.apply(obj));
            toItemOtherKeysMapper.forEach(((keyName, objToKeyMapper) ->
                    item.withString(keyName, objToKeyMapper.apply(obj))));
            toItemArgs.forEach(m -> m.accept(item, obj));
            return item;
        };

        // toAttrMap
        ImmutableList<BiConsumer<ImmutableMap.Builder<String, AttributeValue>, T>> toAttrMapArgs = toAttrMapArgsBuilder.build();
        Function<T, ImmutableMap<String, AttributeValue>> toAttrMapMapper = obj -> {
            ImmutableMap.Builder<String, AttributeValue> attrMapBuilder = ImmutableMap.builder();
            attrMapBuilder.put(partitionKeyName, new AttributeValue(getPartitionKeyVal.apply(obj)));
            attrMapBuilder.put(rangeKeyName, new AttributeValue(getRangeKeyVal.apply(obj)));
            toItemOtherKeysMapper.forEach(((keyName, objToKeyMapper) ->
                    attrMapBuilder.put(keyName, new AttributeValue(objToKeyMapper.apply(obj)))));
            toAttrMapArgs.forEach(m -> m.accept(attrMapBuilder, obj));
            return attrMapBuilder.build();
        };

        // toDynamoValue fromDynamoValue
        ImmutableMap<String, MarshallerItem> fieldMarshallers = fieldMarshallersBuilder.build();
        ImmutableMap<String, UnMarshallerItem> fieldUnMarshallers = fieldUnMarshallersBuilder.build();

        // toAttrValue fromAttrValue
        ImmutableMap<String, MarshallerAttrVal> fieldAttrMarshallers = fieldAttrMarshallersBuilder.build();
        ImmutableMap<String, UnMarshallerAttrVal> fieldAttrUnMarshallers = fieldAttrUnMarshallersBuilder.build();

        // Expression Builder Supplier
        Supplier<ExpressionBuilder> expressionBuilderSupplier = () -> new ExpressionBuilder() {
            private boolean built = false;
            private final Map<String, String> nameMap = Maps.newHashMap();
            private final Map<String, Object> valMap = Maps.newHashMap();
            private final Map<String, String> setUpdates = Maps.newHashMap();
            private final Map<String, String> removeUpdates = Maps.newHashMap();
            private final Map<String, String> addUpdates = Maps.newHashMap();
            private final Map<String, String> deleteUpdates = Maps.newHashMap();
            private final List<String> conditionExpressions = Lists.newArrayList();

            @Override
            public ExpressionBuilder set(String fieldName, Object object) {
                checkState(!built);
                checkState(!setUpdates.containsKey(fieldName));
                setUpdates.put(fieldName,
                        fieldMapping(fieldName) + " = " + valueMapping(fieldName, object));
                return this;
            }

            @Override
            public ExpressionBuilder set(ImmutableList<String> fieldPath, Object object) {
                checkState(!built);
                checkArgument(!fieldPath.isEmpty());
                String fieldMapping = fieldMapping(fieldPath);
                checkState(!addUpdates.containsKey(fieldMapping));
                setUpdates.put(fieldMapping,
                        fieldMapping + " = " + valueMapping(fieldPath, object));
                return this;
            }

            @Override
            public ExpressionBuilder setIncrement(String fieldName, Number increment) {
                checkState(!built);
                checkState(!setUpdates.containsKey(fieldName));
                setUpdates.put(fieldName, String.format("%s = if_not_exists(%s, %s) + %s",
                        fieldMapping(fieldName),
                        fieldMapping(fieldName),
                        constantMapping("zero", 0L),
                        valueMapping(fieldName, increment)));
                return this;
            }

            @Override
            public ExpressionBuilder setExpression(String fieldName, String valueExpression) {
                checkState(!built);
                checkState(!setUpdates.containsKey(fieldName));
                setUpdates.put(fieldName,
                        fieldMapping(fieldName) + " = " + valueExpression);
                return this;
            }

            @Override
            public ExpressionBuilder setExpression(String expression) {
                checkState(!built);
                setUpdates.put(expression, expression);
                return this;
            }

            @Override
            public ExpressionBuilder add(String fieldName, Object object) {
                checkState(!built);
                checkState(!addUpdates.containsKey(fieldName));
                addUpdates.put(fieldName,
                        fieldMapping(fieldName) + " " + valueMapping(fieldName, object));
                return this;
            }

            @Override
            public ExpressionBuilder add(ImmutableList<String> fieldPath, Object object) {
                checkState(!built);
                checkArgument(!fieldPath.isEmpty());
                String fieldMapping = fieldMapping(fieldPath);
                checkState(!addUpdates.containsKey(fieldMapping));
                addUpdates.put(fieldMapping,
                        fieldMapping + " " + valueMapping(fieldPath, object));
                return this;
            }

            @Override
            public ExpressionBuilder remove(String fieldName) {
                checkState(!built);
                checkState(!removeUpdates.containsKey(fieldName));
                removeUpdates.put(fieldName, fieldMapping(fieldName));
                return this;
            }

            @Override
            public ExpressionBuilder remove(ImmutableList<String> fieldPath) {
                checkState(!built);
                checkArgument(!fieldPath.isEmpty());
                String fieldMapping = fieldMapping(fieldPath);
                checkState(!addUpdates.containsKey(fieldMapping));
                removeUpdates.put(fieldMapping, fieldMapping);
                return this;
            }

            @Override
            public ExpressionBuilder delete(String fieldName, Object object) {
                checkState(!built);
                checkState(!deleteUpdates.containsKey(fieldName));
                deleteUpdates.put(fieldName,
                        fieldMapping(fieldName) + " " + valueMapping(fieldName, object));
                return this;
            }

            @Override
            public String fieldMapping(String fieldName) {
                checkState(!built);
                String mappedName = "#" + sanitizeFieldMapping(fieldName);
                nameMap.put(mappedName, fieldName);
                return mappedName;
            }

            @Override
            public String fieldMapping(ImmutableList<String> fieldPath) {
                return fieldPath.stream()
                        .map(this::fieldMapping)
                        .collect(Collectors.joining("."));
            }

            @Override
            public String fieldMapping(String fieldName, String fieldValue) {
                checkState(!built);
                String mappedName = "#" + sanitizeFieldMapping(fieldName);
                nameMap.put(mappedName, fieldValue);
                return mappedName;
            }

            @Override
            public String valueMapping(String fieldName, Object object) {
                checkState(!built);
                Object val;
                if (object instanceof String) {
                    // For partition range keys and strings in general, there is no marshaller
                    val = object;
                } else {
                    Item tempItem = new Item();
                    checkNotNull(fieldMarshallers.get(fieldName), "Unknown field name %s", fieldName)
                            .marshall(object, "tempAttr", tempItem);
                    val = tempItem.get("tempAttr");
                }
                return constantMapping(fieldName, val);
            }

            @Override
            public String constantMapping(String fieldName, Object object) {
                checkState(!built);
                String mappedName = ":" + sanitizeFieldMapping(fieldName);
                valMap.put(mappedName, object);
                return mappedName;
            }

            @Override
            public String valueMapping(ImmutableList<String> fieldPath, Object object) {
                return constantMapping(fieldPath.stream()
                        .map(String::toLowerCase)
                        .collect(Collectors.joining("X")), object);
            }

            @Override
            public ExpressionBuilder condition(String expression) {
                checkState(!built);
                conditionExpressions.add(expression);
                return this;
            }

            @Override
            public ExpressionBuilder conditionExists() {
                checkState(!built);
                conditionExpressions.add("attribute_exists(" + fieldMapping(partitionKeyName) + ")");
                return this;
            }

            @Override
            public ExpressionBuilder conditionNotExists() {
                checkState(!built);
                conditionExpressions.add("attribute_not_exists(" + fieldMapping(partitionKeyName) + ")");
                return this;
            }

            @Override
            public ExpressionBuilder conditionFieldEquals(String fieldName, Object objectOther) {
                checkState(!built);
                conditionExpressions.add(fieldMapping(fieldName) + " = " + valueMapping(fieldName, objectOther));
                return this;
            }

            @Override
            public ExpressionBuilder conditionFieldExists(String fieldName) {
                checkState(!built);
                conditionExpressions.add("attribute_exists(" + fieldMapping(fieldName) + ")");
                return this;
            }

            @Override
            public ExpressionBuilder conditionFieldNotExists(String fieldName) {
                checkState(!built);
                conditionExpressions.add("attribute_not_exists(" + fieldMapping(fieldName) + ")");
                return this;
            }

            @Override
            public Expression build() {
                built = true;
                ArrayList<String> updates = Lists.newArrayList();
                if (!setUpdates.isEmpty()) {
                    updates.add("SET " + String.join(", ", setUpdates.values()));
                }
                if (!addUpdates.isEmpty()) {
                    updates.add("ADD " + String.join(", ", addUpdates.values()));
                }
                if (!removeUpdates.isEmpty()) {
                    updates.add("REMOVE " + String.join(", ", removeUpdates.values()));
                }
                if (!deleteUpdates.isEmpty()) {
                    updates.add("DELETE " + String.join(", ", deleteUpdates.values()));
                }
                final Optional<String> updateOpt = Optional.ofNullable(Strings.emptyToNull(String.join(" ", updates)));
                final Optional<String> conditionOpt = Optional.ofNullable(Strings.emptyToNull(String.join(" AND ", conditionExpressions)));
                final Optional<ImmutableMap<String, String>> nameImmutableMapOpt = nameMap.isEmpty() ? Optional.empty() : Optional.of(ImmutableMap.copyOf(nameMap));
                final Optional<ImmutableMap<String, Object>> valImmutableMapOpt = valMap.isEmpty() ? Optional.empty() : Optional.of(ImmutableMap.copyOf(valMap));
                log.trace("Built dynamo expression: update {} condition {} nameMap {} valKeys {}",
                        updateOpt, conditionOpt, nameImmutableMapOpt, valImmutableMapOpt.map(ImmutableMap::keySet));
                return new Expression() {
                    @Override
                    public Optional<String> updateExpression() {
                        return updateOpt;
                    }

                    @Override
                    public Optional<String> conditionExpression() {
                        return conditionOpt;
                    }

                    @Override
                    public Optional<ImmutableMap<String, String>> nameMap() {
                        return nameImmutableMapOpt;
                    }

                    @Override
                    public Optional<ImmutableMap<String, Object>> valMap() {
                        return valImmutableMapOpt;
                    }

                    @Override
                    public String toString() {
                        return MoreObjects.toStringHelper(this)
                                .add("updateExpression", this.updateExpression())
                                .add("conditionExpression", this.conditionExpression())
                                .add("nameMap", this.nameMap())
                                .add("valMap", this.valMap())
                                .toString();
                    }
                };
            }

            private String sanitizeFieldMapping(String fieldName) {
                return fieldName.replaceAll("(^[^a-z])|[^a-zA-Z0-9]", "x");
            }
        };

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
                partitionKeyName,
                rangeKeyName,
                table,
                index,
                fieldMarshallers,
                fieldUnMarshallers,
                fieldAttrMarshallers,
                fieldAttrUnMarshallers,
                fromItemToCtorArgs,
                fromAttrMapToCtorArgs,
                objCtor,
                toItemMapper,
                toAttrMapMapper,
                expressionBuilderSupplier,
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

    private MarshallerItem findMarshallerItem(Optional<Class> collectionClazz, Class itemClazz) {
        MarshallerItem f = findInClassSet(itemClazz, converters.mip).orElse(gsonMarshallerItem);
        if (collectionClazz.isPresent()) {
            CollectionMarshallerItem fc = findInClassSet(collectionClazz.get(), converters.mic).get();
            return (o, a, i) -> fc.marshall(o, a, i, f);
        } else {
            return f;
        }
    }

    private UnMarshallerItem findUnMarshallerItem(Optional<Class> collectionClazz, Class itemClazz) {
        UnMarshallerItem f = findInClassSet(itemClazz, converters.uip).orElseGet(() -> gsonUnMarshallerItem.apply(itemClazz));
        if (collectionClazz.isPresent()) {
            CollectionUnMarshallerItem fc = findInClassSet(collectionClazz.get(), converters.uic).get();
            return (a, i) -> fc.unmarshall(a, i, f);
        } else {
            return f;
        }
    }

    private MarshallerAttrVal findMarshallerAttrVal(Optional<Class> collectionClazz, Class itemClazz) {
        MarshallerAttrVal f = findInClassSet(itemClazz, converters.map).orElse(gsonMarshallerAttrVal);
        if (collectionClazz.isPresent()) {
            CollectionMarshallerAttrVal fc = findInClassSet(collectionClazz.get(), converters.mac).get();
            return o -> fc.marshall(o, f);
        } else {
            return f;
        }
    }

    private UnMarshallerAttrVal findUnMarshallerAttrVal(Optional<Class> collectionClazz, Class itemClazz) {
        UnMarshallerAttrVal f = findInClassSet(itemClazz, converters.uap).orElseGet(() -> gsonUnMarshallerAttrVal.apply(itemClazz));
        if (collectionClazz.isPresent()) {
            CollectionUnMarshallerAttrVal fc = findInClassSet(collectionClazz.get(), converters.uac).get();
            return a -> fc.unmarshall(a, f);
        } else {
            return f;
        }
    }

    private <T> Optional<T> findInClassSet(Class clazz, ImmutableSet<Map.Entry<Class<?>, T>> set) {
        for (Map.Entry<Class<?>, T> entry : set) {
            if (entry.getKey().isAssignableFrom(clazz)) {
                return Optional.of(entry.getValue());
            }
        }
        return Optional.empty();
    }

    public class SchemaImpl<T> implements TableSchema<T>, IndexSchema<T> {
        private final TableType type;
        private final String[] partitionKeys;
        private final String[] shardKeys;
        private final int shardCount;
        private final String[] rangeKeys;
        private final Field[] partitionKeyFields;
        private final Field[] rangeKeyFields;
        private final String rangePrefix;
        private final String tableName;
        private final String partitionKeyName;
        private final String rangeKeyName;
        private final Table table;
        private final Index index;
        private final ImmutableMap<String, MarshallerItem> fieldMarshallers;
        private final ImmutableMap<String, UnMarshallerItem> fieldUnMarshallers;
        private final ImmutableMap<String, MarshallerAttrVal> fieldAttrMarshallers;
        private final ImmutableMap<String, UnMarshallerAttrVal> fieldAttrUnMarshallers;
        private final Function<Item, Object[]> fromItemToCtorArgs;
        private final Function<Map<String, AttributeValue>, Object[]> fromAttrMapToCtorArgs;
        private final Constructor<T> objCtor;
        private final Function<T, Item> toItemMapper;
        private final Function<T, ImmutableMap<String, AttributeValue>> toAttrMapMapper;
        private final Supplier<ExpressionBuilder> expressionBuilderSupplier;
        private final Function<T, String> partitionKeyValueObjGetter;
        private final Function<Map<String, Object>, String> partitionKeyValueMapGetter;
        private final BiFunction<Map<String, Object>, Integer, String> partitionKeyValueMapShardGetter;

        public SchemaImpl(
                TableType type,
                String[] partitionKeys,
                String[] shardKeys,
                int shardCount,
                String[] rangeKeys,
                Field[] partitionKeyFields,
                Field[] rangeKeyFields,
                String rangePrefix,
                String tableName,
                String partitionKeyName,
                String rangeKeyName,
                Table table,
                Index index,
                ImmutableMap<String, MarshallerItem> fieldMarshallers,
                ImmutableMap<String, UnMarshallerItem> fieldUnMarshallers,
                ImmutableMap<String, MarshallerAttrVal> fieldAttrMarshallers,
                ImmutableMap<String, UnMarshallerAttrVal> fieldAttrUnMarshallers,
                Function<Item, Object[]> fromItemToCtorArgs,
                Function<Map<String, AttributeValue>, Object[]> fromAttrMapToCtorArgs,
                Constructor<T> objCtor, Function<T, Item> toItemMapper,
                Function<T, ImmutableMap<String, AttributeValue>> toAttrMapMapper,
                Supplier<ExpressionBuilder> expressionBuilderSupplier,
                Function<T, String> partitionKeyValueObjGetter,
                Function<Map<String, Object>,
                        String> partitionKeyValueMapGetter,
                BiFunction<Map<String, Object>, Integer, String> partitionKeyValueMapShardGetter) {
            this.type = type;
            this.partitionKeys = partitionKeys;
            this.shardKeys = shardKeys;
            this.shardCount = shardCount;
            this.rangeKeys = rangeKeys;
            this.partitionKeyFields = partitionKeyFields;
            this.rangeKeyFields = rangeKeyFields;
            this.rangePrefix = rangePrefix;
            this.tableName = tableName;
            this.partitionKeyName = partitionKeyName;
            this.rangeKeyName = rangeKeyName;
            this.table = table;
            this.index = index;
            this.fieldMarshallers = fieldMarshallers;
            this.fieldUnMarshallers = fieldUnMarshallers;
            this.fieldAttrMarshallers = fieldAttrMarshallers;
            this.fieldAttrUnMarshallers = fieldAttrUnMarshallers;
            this.fromItemToCtorArgs = fromItemToCtorArgs;
            this.fromAttrMapToCtorArgs = fromAttrMapToCtorArgs;
            this.objCtor = objCtor;
            this.toItemMapper = toItemMapper;
            this.toAttrMapMapper = toAttrMapMapper;
            this.expressionBuilderSupplier = expressionBuilderSupplier;
            this.partitionKeyValueObjGetter = partitionKeyValueObjGetter;
            this.partitionKeyValueMapGetter = partitionKeyValueMapGetter;
            this.partitionKeyValueMapShardGetter = partitionKeyValueMapShardGetter;
        }

        @Override
        public String tableName() {
            return tableName;
        }

        @Override
        public Table table() {
            return table;
        }

        @Override
        public ExpressionBuilder expressionBuilder() {
            return expressionBuilderSupplier.get();
        }

        @Override
        public String indexName() {
            return tableName;
        }

        @Override
        public Index index() {
            return index;
        }

        @Override
        public QueryApi queryApi() {
            return Primary.equals(type) ? table() : index();
        }

        @Override
        public ScanApi scanApi() {
            return Primary.equals(type) ? table() : index();
        }

        @Override
        public PrimaryKey primaryKey(T obj) {
            return new PrimaryKey(partitionKey(obj), rangeKey(obj));
        }

        @Override
        public PrimaryKey primaryKey(Map<String, Object> values) {
            checkState(partitionKeys.length + rangeKeys.length >= values.size(), "Unexpected extra values, partition keys %s range keys %s values %s", partitionKeys, rangeKeys, values);
            return new PrimaryKey(
                    new KeyAttribute(
                            partitionKeyName,
                            partitionKeyValue(values)),
                    new KeyAttribute(
                            rangeKeyName,
                            StringSerdeUtil.mergeStrings(Stream.concat(Stream.of(rangePrefix), Arrays.stream(rangeKeys)
                                            .map(rangeKey -> gson.toJson(checkNotNull(values.get(rangeKey), "Range key missing value for %s", rangeKey))))
                                    .toArray(String[]::new))));
        }

        @Override
        public String partitionKeyName() {
            return partitionKeyName;
        }

        @Override
        public KeyAttribute partitionKey(T obj) {
            return new KeyAttribute(partitionKeyName, partitionKeyValue(obj));
        }

        @Override
        public KeyAttribute partitionKey(Map<String, Object> values) {
            return new KeyAttribute(
                    partitionKeyName,
                    partitionKeyValue(values));
        }

        @Override
        public KeyAttribute shardKey(int shard) {
            checkArgument(partitionKeys.length > 0, "Partition keys are required, call shardKey(shard, values) instead");
            return shardKey(shard, Map.of());
        }

        @Override
        public KeyAttribute shardKey(int shard, Map<String, Object> values) {
            checkArgument(shardKeys.length > 0, "Cannot construct a shard key for schema with no shardKeys defined");
            checkArgument(shard >= 0, "Shard number " + shard + " cannot be negative");
            checkArgument(shard < shardCount, "Shard number starts with zero and must be less than the maximum shard count of " + shardCount);
            return new KeyAttribute(
                    partitionKeyName,
                    partitionKeyValueMapShardGetter.apply(values, shard));
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
        public KeyAttribute rangeKey(T obj) {
            return new KeyAttribute(
                    rangeKeyName,
                    StringSerdeUtil.mergeStrings(Stream.concat(Stream.of(rangePrefix), Arrays.stream(rangeKeyFields)
                                    .map(rangeKeyField -> {
                                        try {
                                            return gson.toJson(checkNotNull(rangeKeyField.get(obj),
                                                    "Range key value null, should add @NonNull on all keys for class %s", obj));
                                        } catch (IllegalAccessException ex) {
                                            throw new RuntimeException(ex);
                                        }
                                    }))
                            .toArray(String[]::new)));
        }

        @Override
        public KeyAttribute rangeKey(Map<String, Object> values) {
            checkState(rangeKeys.length == values.size(), "Unexpected extra values, range keys %s values %s", rangeKeys, values);
            return new KeyAttribute(
                    rangeKeyName,
                    StringSerdeUtil.mergeStrings(Stream.concat(Stream.of(rangePrefix), Arrays.stream(rangeKeys)
                                    .map(rangeKey -> gson.toJson(checkNotNull(values.get(rangeKey), "Range key missing value for %s", rangeKey))))
                            .toArray(String[]::new)));
        }

        @Override
        public KeyAttribute rangeKeyPartial(Map<String, Object> values) {
            return new KeyAttribute(
                    rangeKeyName,
                    StringSerdeUtil.mergeStrings(Stream.concat(Stream.of(rangePrefix), Arrays.stream(rangeKeys)
                                    .map(values::get)
                                    .takeWhile(Objects::nonNull)
                                    .map(gson::toJson))
                            .toArray(String[]::new)));
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
        public Object toDynamoValue(String fieldName, Object object) {
            if (object == null) {
                return null;
            }
            Item tempItem = new Item();
            checkNotNull(fieldMarshallers.get(fieldName), "Unknown field name %s", fieldName)
                    .marshall(object, "tempAttr", tempItem);
            return tempItem.get("tempAttr");
        }

        @Override
        public Object fromDynamoValue(String fieldName, Object object) {
            if (object == null) {
                return null;
            }
            Item tempItem = new Item();
            tempItem.with("tempAttr", object);
            return checkNotNull(fieldUnMarshallers.get(fieldName), "Unknown field name %s", fieldName)
                    .unmarshall("tempAttr", tempItem);
        }

        @Override
        public AttributeValue toAttrValue(String fieldName, Object object) {
            return fieldAttrMarshallers.get(fieldName).marshall(object);
        }

        @Override
        public Object fromAttrValue(String fieldName, AttributeValue attrVal) {
            return fieldAttrUnMarshallers.get(fieldName).unmarshall(attrVal);
        }

        @Override
        public Item toItem(T object) {
            if (object == null) {
                return null;
            }
            return toItemMapper.apply(object);
        }

        @Override
        public T fromItem(Item item) {
            // TODO check consistency of returning values. prevent user from updating fields that are also pk or sk in GSI or LSI
            if (item == null) {
                return null;
            }
            Object[] args = fromItemToCtorArgs.apply(item);
            try {
                return objCtor.newInstance(args);
            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException |
                     InvocationTargetException ex) {
                throw new RuntimeException("Failed to construct, item: " + item.toJSON() + " objCtor: " + objCtor + " args: " + Arrays.toString(args), ex);
            }
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
            if (attrMap == null) {
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
        public String upsertExpression(T object, Map<String, String> nameMap, Map<String, Object> valMap, ImmutableSet<String> skipFieldNames, String additionalExpression) {
            return upsertExpression(
                    object,
                    nameMap,
                    (key, val) -> valMap.put(":" + key, val),
                    skipFieldNames,
                    additionalExpression);
        }

        @Override
        public String upsertExpressionAttrVal(T object, Map<String, String> nameMap, Map<String, AttributeValue> valMap, ImmutableSet<String> skipFieldNames, String additionalExpression) {
            return upsertExpression(
                    object,
                    nameMap,
                    (key, val) -> valMap.put(":" + key, toAttrValue(key, val)),
                    skipFieldNames,
                    additionalExpression);
        }

        private String upsertExpression(T object, Map<String, String> nameMap, BiConsumer<String, Object> valMapPutter, ImmutableSet<String> skipFieldNames, String additionalExpression) {
            List<String> setUpdates = Lists.newArrayList();
            toItemMapper.apply(object).attributes().forEach(entry -> {
                if (partitionKeyName.equals(entry.getKey()) || rangeKeyName.equals(entry.getKey())) {
                    return;
                }
                if (skipFieldNames.contains(entry.getKey())) {
                    return;
                }
                nameMap.put("#" + entry.getKey(), entry.getKey());
                valMapPutter.accept(entry.getKey(), entry.getValue());
                setUpdates.add("#" + entry.getKey() + " = " + ":" + entry.getKey());
            });
            return "SET " + String.join(", ", setUpdates) + additionalExpression;
        }

        @Override
        public String serializeLastEvaluatedKey(Map<String, AttributeValue> lastEvaluatedKey) {
            return gson.toJson(Maps.transformValues(lastEvaluatedKey, AttributeValue::getS));
        }

        @Override
        public PrimaryKey toExclusiveStartKey(String serializedlastEvaluatedKey) {
            Map<String, String> attributes = gson.fromJson(serializedlastEvaluatedKey, new TypeToken<Map<String, String>>() {
            }.getType());
            return toExclusiveStartKey(attributes);
        }

        private PrimaryKey toExclusiveStartKey(Map<String, String> attributes) {
            return new PrimaryKey(attributes.entrySet().stream()
                    .map(e -> new KeyAttribute(e.getKey(), e.getValue()))
                    .toArray(KeyAttribute[]::new));
        }

        @Override
        public String serializeShardedLastEvaluatedKey(Optional<Map<String, AttributeValue>> lastEvaluatedKeyOpt, int shard) {
            return gson.toJson(new ShardAndAttributes(
                    shard,
                    lastEvaluatedKeyOpt
                            .map(lastEvaluatedKey -> Maps.transformValues(lastEvaluatedKey, AttributeValue::getS))
                            .orElse(null)));
        }

        @Override
        public ShardAndExclusiveStartKey wrapShardedLastEvaluatedKey(Optional<Map<String, AttributeValue>> lastEvaluatedKeyOpt, int shard) {
            return new ShardAndExclusiveStartKey(
                    shard,
                    lastEvaluatedKeyOpt
                            .map(lastEvaluatedKey -> Maps.transformValues(lastEvaluatedKey, AttributeValue::getS))
                            .map(this::toExclusiveStartKey));
        }

        @Override
        public String serializeShardedLastEvaluatedKey(ShardAndExclusiveStartKey shardAndExclusiveStartKey) {
            return gson.toJson(new ShardAndAttributes(
                    shardAndExclusiveStartKey.getShard(),
                    shardAndExclusiveStartKey.getExclusiveStartKey()
                            .map(primaryKey -> primaryKey.getComponents().stream()
                                    .collect(Collectors.toMap(
                                            Attribute::getName,
                                            // Keys must all be strings
                                            keyAttribute -> (String) keyAttribute.getValue())))
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
