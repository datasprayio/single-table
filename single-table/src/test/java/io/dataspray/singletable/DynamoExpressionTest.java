// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import io.dataspray.singletable.builder.UpdateBuilder;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.util.UUID;

import static io.dataspray.singletable.TableType.Primary;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Slf4j
public class DynamoExpressionTest extends AbstractDynamoTest {

    @Value
    @Builder(toBuilder = true)
    @AllArgsConstructor
    @DynamoTable(type = Primary, partitionKeys = "id", rangePrefix = "prefixPrimary", rangeKeys = "rk")
    public static class Data {
        private final String id;
        private final String rk;
        private final String f1;
        private final long f2;
        private final String f3;
        private final Integer f4;
    }

    @Test(timeout = 20_000L)
    public void test() throws Exception {
        TableSchema<Data> primary = mapper.parseTableSchema(Data.class);

        UpdateBuilder<Data> updateBuilder = primary.update()
                .set("f1", "CHANGED")
                .conditionExists()
                .conditionFieldEquals("f2", 4L)
                .conditionFieldExists("f3")
                .conditionFieldNotExists("f1");
        log.info("updateBuilder: {}", updateBuilder);
        UpdateItemRequest.Builder updateItemRequestBuilder = updateBuilder.builder();

        assertExpression(primary, updateItemRequestBuilder, putData(primary, Data.builder()
                .id(UUID.randomUUID().toString())
                .rk(UUID.randomUUID().toString())
                .f2(4L)
                .f3("asdf")
                .f4(1)
                .build())
                .toBuilder().f1("CHANGED").build());

        assertExpressionConditionFails(primary, updateItemRequestBuilder, putData(primary, Data.builder()
                .id(UUID.randomUUID().toString())
                .rk(UUID.randomUUID().toString())
                .f2(5L) // Incorrect
                .f3("qwe")
                .f4(3)
                .build()));

        assertExpressionConditionFails(primary, updateItemRequestBuilder, putData(primary, Data.builder()
                .id(UUID.randomUUID().toString())
                .rk(UUID.randomUUID().toString())
                .f2(4L)
                // f3 missing
                .f4(7)
                .build()));

        assertExpressionConditionFails(primary, updateItemRequestBuilder, putData(primary, Data.builder()
                .id(UUID.randomUUID().toString())
                .rk(UUID.randomUUID().toString())
                .f1("htg") // Should be missing
                .f2(4L)
                .f3("qwe")
                .build()));
    }

    <T> void assertExpression(Schema<T> schema, UpdateItemRequest.Builder builder, T expectedData) {
        client.updateItem(builder.key(schema.primaryKey(expectedData)).build());
        T actualData = schema.fromAttrMap(client.getItem(b -> b
                .tableName(schema.tableName())
                .key(schema.primaryKey(expectedData))).item());
        assertEquals(expectedData, actualData);
    }

    <T> void assertExpressionConditionFails(Schema<T> schema, UpdateItemRequest.Builder builder, T expectedData) {
        try {
            client.updateItem(builder.key(schema.primaryKey(expectedData)).build());
            fail("Expected ConditionalCheckFailedException");
        } catch (ConditionalCheckFailedException ex) {
            // Expected
        }
    }
}
