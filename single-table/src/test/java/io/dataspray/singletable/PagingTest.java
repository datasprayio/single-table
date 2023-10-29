// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.dataspray.singletable.TableType.Primary;
import static org.junit.Assert.assertEquals;

@Slf4j
public class PagingTest extends AbstractDynamoTest {

    @Value
    @Builder(toBuilder = true)
    @AllArgsConstructor
    @DynamoTable(type = Primary, partitionKeys = "f1", rangePrefix = "prefix", rangeKeys = "f2")
    public static class Data {
        private final String f1;
        private final String f2;
    }

    @Test(timeout = 20_000L)
    public void testCursor() throws Exception {
        TableSchema<Data> schema = mapper.parseTableSchema(Data.class);

        String partitionKey = UUID.randomUUID().toString();
        Data data1 = putData(schema, Data.builder()
                .f1(partitionKey)
                .f2(UUID.randomUUID().toString())
                .build());
        Data data2 = putData(schema, Data.builder()
                .f1(partitionKey)
                .f2(UUID.randomUUID().toString())
                .build());
        Data data3 = putData(schema, Data.builder()
                .f1(partitionKey)
                .f2(UUID.randomUUID().toString())
                .build());
        // Other item with different partition key
        putData(schema, Data.builder()
                .f1(UUID.randomUUID().toString())
                .f2(UUID.randomUUID().toString())
                .build());

        long requestCount = 0;
        Set<Data> items = Sets.newHashSet();
        Optional<String> cursorOpt = Optional.empty();
        do {
            // Prepare request
            QueryRequest.Builder builder = QueryRequest.builder()
                    .tableName(schema.tableName())
                    // Query by partition key
                    .keyConditions(schema.attrMapToConditions(schema.partitionKey(Map.of(
                            "f1", partitionKey))))
                    .limit(2);
            cursorOpt.ifPresent(exclusiveStartKey -> builder.exclusiveStartKey(schema.toExclusiveStartKey(exclusiveStartKey)));

            // Perform request
            QueryResponse response = client.query(builder.build());
            requestCount++;

            // Retrieve cursor
            cursorOpt = schema.serializeLastEvaluatedKey(response.lastEvaluatedKey());

            // Process results
            response.items().stream()
                    .map(schema::fromAttrMap)
                    .forEachOrdered(items::add);
        } while (cursorOpt.isPresent());

        assertEquals(Set.of(data1, data2, data3), items);
        assertEquals(2, requestCount);
    }

    @Test(timeout = 20_000L)
    public void testPaginator() throws Exception {
        TableSchema<Data> schema = mapper.parseTableSchema(Data.class);

        String partitionKey = UUID.randomUUID().toString();
        Data data1 = putData(schema, Data.builder()
                .f1(partitionKey)
                .f2(UUID.randomUUID().toString())
                .build());
        Data data2 = putData(schema, Data.builder()
                .f1(partitionKey)
                .f2(UUID.randomUUID().toString())
                .build());
        Data data3 = putData(schema, Data.builder()
                .f1(partitionKey)
                .f2(UUID.randomUUID().toString())
                .build());
        // Other item with different partition key
        putData(schema, Data.builder()
                .f1(UUID.randomUUID().toString())
                .f2(UUID.randomUUID().toString())
                .build());

        // Perform request
        Set<Data> items = client.queryPaginator(QueryRequest.builder()
                        .tableName(schema.tableName())
                        // Query by partition key
                        .keyConditions(schema.attrMapToConditions(schema.partitionKey(Map.of(
                                "f1", partitionKey))))
                        .limit(2)
                        .build())
                .items()
                .stream()
                .map(schema::fromAttrMap)
                .collect(Collectors.toSet());

        assertEquals(Set.of(data1, data2, data3), items);
    }
}
