// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.stream.StreamSupport;

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

        log.info("Table description {}", primary.table().describe());
        log.info("primary.toItem(data) {}", primary.toItem(data));
        log.info("primary.primaryKey(data) {}", primary.primaryKey(data));
        assertNull(primary.fromItem(primary.table().putItem(new PutItemSpec()
                .withItem(primary.toItem(data)).withReturnValues(ReturnValue.ALL_OLD)).getItem()));
        assertEquals(data, primary.fromItem(primary.table().getItem(primary.primaryKey(data))));
        assertEquals(Optional.of(data), StreamSupport.stream(lsi1.index().query(lsi1.partitionKey(data)).pages().spliterator(), false).flatMap(p -> StreamSupport.stream(p.spliterator(), false)).map(lsi1::fromItem).findAny());
        assertEquals(Optional.of(data), StreamSupport.stream(gsi1.index().query(gsi1.partitionKey(data)).pages().spliterator(), false).flatMap(p -> StreamSupport.stream(p.spliterator(), false)).map(gsi1::fromItem).findAny());
        assertEquals(Optional.of(data), StreamSupport.stream(gsi2.index().query(gsi2.partitionKey(data)).pages().spliterator(), false).flatMap(p -> StreamSupport.stream(p.spliterator(), false)).map(gsi2::fromItem).findAny());
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

        PrimaryKey primaryKey = mapperNullable.primaryKey(dataNullWithNull);

        assertNull(mapperNullable.fromItem(mapperNullable.table().putItem(new PutItemSpec()
                .withItem(mapperNullable.toItem(dataNullWithNull)).withReturnValues(ReturnValue.ALL_OLD)).getItem()));
        assertEquals(dataNullWithNull, mapperNullable.fromItem(
                mapperNullable.table().getItem(primaryKey)));

        // Circumvent detection of duplicate schema prefix
        ((DynamoMapperImpl) mapper).rangePrefixToDynamoTable.clear();
        // Get same schema with all fields NonNull this time
        TableSchema<DataNonNull> mapperNonNull = mapper.parseTableSchema(DataNonNull.class);

        DataNonNull dataNonNull = new DataNonNull("myId", "", 0L, ImmutableMap.of(), Instant.EPOCH);

        assertEquals(primaryKey, mapperNonNull.primaryKey(dataNonNull));
        assertEquals(dataNonNull, mapperNonNull.fromItem(
                mapperNonNull.table().getItem(primaryKey)));

        assertEquals(dataNonNull, mapperNonNull.fromItem(mapperNonNull.table().putItem(new PutItemSpec()
                .withItem(mapperNonNull.toItem(dataNonNull)).withReturnValues(ReturnValue.ALL_OLD)).getItem()));
        DataNullable dataNullWithNonNull = new DataNullable("myId", null, 0L, ImmutableMap.of(), Instant.EPOCH);
        assertEquals(dataNullWithNonNull, mapperNullable.fromItem(
                mapperNullable.table().getItem(primaryKey)));
    }
}
