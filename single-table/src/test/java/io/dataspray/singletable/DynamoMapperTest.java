// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.dataspray.singletable.TableType.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@Slf4j
public class DynamoMapperTest extends AbstractDynamoTest {

    @Value
    @AllArgsConstructor
    @DynamoTable(type = Primary, partitionKeys = {"f1", "f2"}, rangePrefix = "prefixPrimary", rangeKeys = {"f3", "f4", "f5"})
    @DynamoTable(type = Lsi, indexNumber = 1, partitionKeys = {"f1", "f2"}, rangePrefix = "prefixLsi1", rangeKeys = {"f5", "f6"})
    @DynamoTable(type = Gsi, indexNumber = 1, partitionKeys = {"f3", "f4", "f5"}, rangePrefix = "prefixGsi1", rangeKeys = {"f1", "f2"})
    @DynamoTable(type = Gsi, indexNumber = 2, partitionKeys = {"f1", "f3"}, rangePrefix = "prefixGsi2", rangeKeys = {"f2", "f4"})
    public static class Data {
        @NonNull
        private final String f1;
        @NonNull
        private final long f2;
        @NonNull
        private final String f3;
        @NonNull
        private final Integer f4;
        @NonNull
        private final Instant f5;
        @NonNull
        private final String f6;
    }

    @Test(timeout = 20_000L)
    public void test() throws Exception {
        TableSchema<Data> primary = mapper.parseTableSchema(Data.class);
        IndexSchema<Data> lsi1 = mapper.parseLocalSecondaryIndexSchema(1, Data.class);
        IndexSchema<Data> gsi1 = mapper.parseGlobalSecondaryIndexSchema(1, Data.class);
        IndexSchema<Data> gsi2 = mapper.parseGlobalSecondaryIndexSchema(2, Data.class);


        Data data = new Data("f1", 2L, "f3", 4, Instant.ofEpochMilli(5), "f6");

        log.info("Table description {}", client.describeTable(DescribeTableRequest.builder()
                .tableName(primary.tableName()).build()));
        log.info("primary.toAttrMap(data) {}", primary.toAttrMap(data));
        log.info("primary.primaryKey(data) {}", primary.primaryKey(data));
        assertNull(primary.fromAttrMap(client.putItem(PutItemRequest.builder()
                        .tableName(primary.tableName())
                        .item(primary.toAttrMap(data))
                        .returnValues(ReturnValue.ALL_OLD).build())
                .attributes()));
        assertEquals(data, primary.fromAttrMap(client.getItem(GetItemRequest.builder()
                        .tableName(primary.tableName())
                        .key(primary.primaryKey(data)).build())
                .item()));
        assertEquals(ImmutableList.of(data), client.query(QueryRequest.builder()
                        .tableName(lsi1.tableName())
                        .indexName(lsi1.indexName())
                        .keyConditions(lsi1.attrMapToConditions(lsi1.primaryKey(data))).build()).items().stream()
                .map(lsi1::fromAttrMap).collect(Collectors.toList()));
        assertEquals(ImmutableList.of(data), client.query(QueryRequest.builder()
                        .tableName(gsi1.tableName())
                        .indexName(gsi1.indexName())
                        .keyConditions(gsi1.attrMapToConditions(gsi1.primaryKey(data))).build()).items().stream()
                .map(gsi1::fromAttrMap).collect(Collectors.toList()));
        assertEquals(ImmutableList.of(data), client.query(QueryRequest.builder()
                        .tableName(gsi2.tableName())
                        .indexName(gsi2.indexName())
                        .keyConditions(gsi2.attrMapToConditions(gsi2.primaryKey(data))).build()).items().stream()
                .map(gsi2::fromAttrMap).collect(Collectors.toList()));
    }

    @Value
    @AllArgsConstructor
    @DynamoTable(type = Primary, partitionKeys = {"id"}, rangePrefix = "prefixDataNonNullNullableTest")
    public static class DataNullable {
        @NonNull
        private final String id;
        private final String f1;
        private final Long f2;
        private final ImmutableMap<String, String> f3;
        private final Instant f4;
    }

    @Value
    @AllArgsConstructor
    @DynamoTable(type = Primary, partitionKeys = {"id"}, rangePrefix = "prefixDataNonNullNullableTest")
    public static class DataNonNull {
        @NonNull
        private final String id;
        @NonNull
        @InitWithDefault
        private final String f1;
        @NonNull
        @InitWithDefault
        private final Long f2;
        @NonNull
        @InitWithDefault
        private final ImmutableMap<String, String> f3;
        @NonNull
        @InitWithDefault
        private final Instant f4;
    }

    @Test(timeout = 20_000L)
    public void testNullableToNonNull() throws Exception {
        TableSchema<DataNullable> mapperNullable = mapper.parseTableSchema(DataNullable.class);

        DataNullable dataNullWithNull = new DataNullable("myId", null, null, null, null);

        Map<String, AttributeValue> primaryKey = mapperNullable.primaryKey(dataNullWithNull);

        assertNull(mapperNullable.fromAttrMap(client.putItem(PutItemRequest.builder()
                        .tableName(mapperNullable.tableName())
                        .item(mapperNullable.toAttrMap(dataNullWithNull))
                        .returnValues(ReturnValue.ALL_OLD).build())
                .attributes()));
        assertEquals(dataNullWithNull, mapperNullable.fromAttrMap(
                client.getItem(GetItemRequest.builder()
                                .tableName(mapperNullable.tableName())
                                .key(primaryKey).build())
                        .item()));

        // Circumvent detection of duplicate schema prefix
        mapper.rangePrefixToDynamoTable.clear();
        // Get same schema with all fields NonNull this time
        TableSchema<DataNonNull> mapperNonNull = mapper.parseTableSchema(DataNonNull.class);

        DataNonNull dataNonNull = new DataNonNull("myId", "", 0L, ImmutableMap.of(), Instant.EPOCH);

        assertEquals(primaryKey, mapperNonNull.primaryKey(dataNonNull));
        assertEquals(dataNonNull, mapperNonNull.fromAttrMap(
                client.getItem(GetItemRequest.builder()
                                .tableName(mapperNonNull.tableName())
                                .key(primaryKey).build())
                        .item()));

        assertEquals(dataNonNull, mapperNonNull.fromAttrMap(client.putItem(PutItemRequest.builder()
                        .tableName(mapperNonNull.tableName())
                        .item(mapperNonNull.toAttrMap(dataNonNull))
                        .returnValues(ReturnValue.ALL_OLD)
                        .build())
                .attributes()));
        DataNullable dataNullWithNonNull = new DataNullable("myId", null, 0L, ImmutableMap.of(), Instant.EPOCH);
        assertEquals(dataNullWithNonNull, mapperNullable.fromAttrMap(
                client.getItem(GetItemRequest.builder()
                                .tableName(mapperNonNull.tableName())
                                .key(primaryKey).build())
                        .item()));
    }


    @Value
    @AllArgsConstructor
    @DynamoTable(type = Primary, partitionKeys = {"f2"}, shardKeys = {"f1"}, shardCount = 20, rangePrefix = "prefixDataShardedTestPrimary", rangeKeys = {"f3"})
    @DynamoTable(type = Gsi, indexNumber = 1, shardKeys = {"f1", "f2"}, shardCount = 20, rangeKeys = {"f3"}, rangePrefix = "prefixDataShardedTestGsi")
    public static class DataSharded {
        @NonNull
        String f1;
        @NonNull
        String f2;
        @NonNull
        String f3;
    }

    @Test(timeout = 20_000L)
    public void testSharded() throws Exception {
        TableSchema<DataSharded> primary = mapper.parseTableSchema(DataSharded.class);
        IndexSchema<DataSharded> gsi = mapper.parseGlobalSecondaryIndexSchema(1, DataSharded.class);

        DataSharded data1 = new DataSharded("1-1", "1-2", "1-3");
        DataSharded data2 = new DataSharded("2-1", "2-2", "2-3");
        DataSharded data3 = new DataSharded("3-1", "3-2", "3-3");

        log.info("Table description {}", client.describeTable(DescribeTableRequest.builder()
                        .tableName(primary.tableName()).build())
                .table());
        log.info("primary.toItem(data1) {}", primary.toAttrMap(data1));
        log.info("primary.primaryKey(data1) {}", primary.primaryKey(data1));
        log.info("gsi.toItem(data1) {}", gsi.toAttrMap(data1));
        log.info("gsi.primaryKey(data1) {}", gsi.primaryKey(data1));
        assertNull(primary.fromAttrMap(client.putItem(PutItemRequest.builder()
                        .tableName(primary.tableName())
                        .item(primary.toAttrMap(data1))
                        .returnValues(ReturnValue.ALL_OLD).build())
                .attributes()));
        assertNull(primary.fromAttrMap(client.putItem(PutItemRequest.builder()
                        .tableName(primary.tableName())
                        .item(primary.toAttrMap(data2))
                        .returnValues(ReturnValue.ALL_OLD).build())
                .attributes()));
        assertNull(primary.fromAttrMap(client.putItem(PutItemRequest.builder()
                        .tableName(primary.tableName())
                        .item(primary.toAttrMap(data3))
                        .returnValues(ReturnValue.ALL_OLD).build())
                .attributes()));
        assertEquals(data1, primary.fromAttrMap(client.getItem(b -> b
                        .tableName(primary.tableName())
                        .key(primary.primaryKey(data1)))
                .item()));
        assertEquals(ImmutableList.of(data1), client.query(b -> b
                        .tableName(gsi.tableName())
                        .indexName(gsi.indexName())
                        .keyConditions(gsi.attrMapToConditions(gsi.partitionKey(data1))))
                .items().stream().map(gsi::fromAttrMap).collect(ImmutableList.toImmutableList()));

        assertEquals(new ShardPageResult<>(ImmutableList.of(data1), Optional.empty()), singleTable.fetchShardNextPage(client, primary, Optional.empty(), 2, Map.of("f2", data1.getF2())));
        assertEquals(new ShardPageResult<>(ImmutableList.of(data3, data2, data1), Optional.empty()), singleTable.fetchShardNextPage(client, gsi, Optional.empty(), 4));
        assertEquals(ImmutableList.of(data3, data2, data1), singleTable.fetchShardNextPage(client, gsi, Optional.empty(), 3).getItems());
        assertEquals(ImmutableList.of(data3, data2), singleTable.fetchShardNextPage(client, gsi, Optional.empty(), 2).getItems());
        assertEquals(new ShardPageResult<>(ImmutableList.of(data1), Optional.empty()), singleTable.fetchShardNextPage(client, gsi, Optional.of("{\"s\":9,\"d\":{\"gsipk1\":\"shard-9\",\"gsisk1\":\"prefixDataShardedTestGsi:\\\"2-3\\\"\",\"sk\":\"prefixDataShardedTestPrimary:\\\"2-3\\\"\",\"pk\":\"\\\"2-2\\\":shard-1\"}}"), 2));
    }
}
