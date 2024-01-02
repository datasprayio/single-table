// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static io.dataspray.singletable.TableType.Primary;
import static org.junit.Assert.assertEquals;

@Slf4j
@RunWith(Parameterized.class)
public class DynamoMapperConversionTest extends AbstractDynamoTest {

    private TableSchema<Data> schema;

    private final String fieldName;
    private final Object example;

    @Value
    @Builder(toBuilder = true)
    @AllArgsConstructor
    @DynamoTable(type = Primary, partitionKeys = "id", rangePrefix = "data")
    public static class Data {
        @NonNull
        private final String id;
        private final Date date;
        private final Boolean boolObj;
        private final boolean boolPri;
        private final byte bytePri;
        private final Byte byteObj;
        private final short shortPri;
        private final Short shortObj;
        private final int intPri;
        private final Integer integer;
        private final long longPri;
        private final Long longObj;
        private final float floatPri;
        private final Float floatObj;
        private final double doublePri;
        private final Double doubleObj;
        private final String String;
        private final UUID UUID;
        private final ByteBuffer ByteBuffer;
        private final byte[] byteArray;
        private final Byte[] byteArrayObj;
        private final Instant instant;
        private final List<UUID> list;
        private final Map<String, Boolean> map;
        @NonNull
        private final Set<Instant> set;
        private final ImmutableList<String> immutableList;
        private final ImmutableMap<String, Long> immutableMap;
        @NonNull
        private final ImmutableSet<String> immutableSet;
        private final BigDecimal bigDecimal;
        private final BigInteger bigInteger;
        private final InnerData inner;
        private final List<InnerData> innerList;
        private final ImmutableList<InnerData> innerImmutableList;
    }

    @Value
    @Builder(toBuilder = true)
    @AllArgsConstructor
    public static class InnerData {
        @NonNull
        private final String id;
        private final Boolean boolObj;
        private final ImmutableSet<String> immutableSet;
    }

    @Parameters(name = "{0} {1}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"date", Date.from(Instant.now().plus(300, ChronoUnit.DAYS))},
                {"boolObj", Boolean.TRUE},
                {"boolPri", true},
                {"bytePri", (byte) 7},
                {"byteObj", (byte) 7},
                {"shortPri", (short) 7},
                {"shortObj", (short) 7},
                {"intPri", 7},
                {"integer", 7},
                {"longPri", 7L},
                {"longObj", 7L},
                {"floatPri", 7f},
                {"floatObj", 7f},
                {"doublePri", 7.7d},
                {"doubleObj", 7.7d},
                {"String", "myString 123"},
                {"UUID", UUID.randomUUID()},
                {"ByteBuffer", ByteBuffer.wrap(new byte[]{6, 2, -34, 127})},
                {"byteArray", new byte[]{6, 2, -34, 127}},
                {"byteArrayObj", new Byte[]{6, 2, -34, 127}},
                {"instant", Instant.now().plus(300, ChronoUnit.DAYS)},
                {"list", ImmutableList.of(UUID.randomUUID(), UUID.randomUUID())},
                {"map", ImmutableMap.of("a", true, "c", false, "b", true)},
                {"set", ImmutableSet.of(Instant.now(), Instant.now().plus(1, ChronoUnit.HOURS), Instant.now().minus(3, ChronoUnit.SECONDS))},
                {"immutableList", ImmutableList.of("a", "c", "b")},
                {"immutableMap", ImmutableMap.of("a", 1L, "c", 3L, "b", 2L)},
                {"immutableSet", ImmutableSet.of("a", "c", "b")},
                {"immutableList", ImmutableList.of()},
                {"immutableMap", ImmutableMap.of()},
                {"immutableSet", ImmutableSet.of()},
                {"bigDecimal", new BigDecimal("3243289.342849032480932")},
                {"bigInteger", new BigInteger("32432893428490324809320000")},
                {"inner", new InnerData("asdf", null, ImmutableSet.of("asdf"))},
                {"innerList", List.of(new InnerData("asdf", null, ImmutableSet.of("asdf")))},
                {"innerImmutableList", ImmutableList.of(new InnerData("asdf", null, ImmutableSet.of("asdf")))}
        });
    }

    public DynamoMapperConversionTest(String fieldName, Object example) {
        this.fieldName = fieldName;
        this.example = example;
    }

    @Before
    public void setup() throws Exception {
        log.info("Testing {} {}", fieldName, example);
        schema = mapper.parseTableSchema(Data.class);
    }

    @Test(timeout = 20_000L)
    public void fromAttrMapToAttrMap() throws Exception {
        Data dataExpected = getExpectedData();
        log.info("dataExpected: {}", dataExpected);

        Map<String, AttributeValue> itemExpected = schema.toAttrMap(dataExpected);
        log.info("itemExpected: {}", itemExpected);
        client.putItem(PutItemRequest.builder()
                .tableName(schema.tableName())
                .item(itemExpected).build());

        Map<String, AttributeValue> itemActual = client.getItem(GetItemRequest.builder()
                        .tableName(schema.tableName())
                        .key(schema.primaryKey(Map.of("id", dataExpected.getId()))).build())
                .item();
        log.info("itemActual: {}", itemActual);
        Data dataActual = schema.fromAttrMap(itemActual);
        log.info("dataActual: {}", dataActual);

        assertEquals(dataExpected, dataActual);

        // We cannot assertEquals(itemExpected, itemActual);
        // This is because Dynamo doesn't preserve number fields as is:
        // Float and Double supplied as "0.0" will be returned back as "0"
    }

    private Data getExpectedData() throws Exception {
        Data.DataBuilder dataBuilder = Data.builder();
        dataBuilder.id(UUID.randomUUID().toString())
                .set(Sets.newHashSet())
                .immutableSet(ImmutableSet.of());
        Arrays.stream(dataBuilder.getClass().getMethods())
                .filter(m -> m.getName().equals(fieldName))
                .findAny()
                .orElseThrow()
                .invoke(dataBuilder, example);
        return dataBuilder.build();
    }
}
