// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.amazonaws.services.dynamodbv2.document.BatchGetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.Uninterruptibles;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

class DynamoUtil {

    private static final long START_MS = 100L;
    private static final long SLEEP_MULTIPLE = 4L;

    private final DynamoDB dynamoDoc;

    DynamoUtil(DynamoDB dynamoDoc) {
        this.dynamoDoc = dynamoDoc;
    }

    public Stream<Item> retryUnprocessed(BatchGetItemOutcome outcome) {
        Stream<Item> items = outcome.getTableItems()
                .values()
                .stream()
                .flatMap(Collection::stream);
        if (outcome.getUnprocessedKeys().isEmpty()) {
            return items;
        }

        long sleepNext = START_MS;
        do {
            Uninterruptibles.sleepUninterruptibly(sleepNext, TimeUnit.MILLISECONDS);
            sleepNext *= SLEEP_MULTIPLE;

            outcome = dynamoDoc.batchGetItemUnprocessed(outcome.getUnprocessedKeys());
            if (!outcome.getTableItems().isEmpty()) {
                items = Stream.concat(items, outcome.getTableItems()
                        .values()
                        .stream()
                        .flatMap(Collection::stream));
            }
        } while (!outcome.getUnprocessedKeys().isEmpty());

        return items;
    }

    public void retryUnprocessed(BatchWriteItemOutcome outcome) {
        long sleepNext = START_MS;
        while (!outcome.getUnprocessedItems().isEmpty()) {
            Uninterruptibles.sleepUninterruptibly(sleepNext, TimeUnit.MILLISECONDS);
            sleepNext *= SLEEP_MULTIPLE;

            outcome = dynamoDoc.batchWriteItemUnprocessed(outcome.getUnprocessedItems());
        }
    }

    public int deterministicPartition(String input, int partitionCount) {
        return Math.abs(Hashing.murmur3_32().hashString(input, Charsets.UTF_8).asInt() % partitionCount);
    }
}
