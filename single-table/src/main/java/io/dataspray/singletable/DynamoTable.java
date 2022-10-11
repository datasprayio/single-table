// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target(TYPE)
@Retention(RUNTIME)
@Repeatable(DynamoTable.DynamoTables.class)
public @interface DynamoTable {

    TableType type();

    /** For GSI and LSI type only, specify which index starting with 1 */
    int indexNumber() default -1;

    /** Partition keys to be compounded together. Lsi should match Primary's keys */
    String[] partitionKeys() default {};

    /** Keys to be hashed together to form a partition. Do not modify after usage, carefully plan ahead. */
    String[] shardKeys() default {};

    /** If using shardKeys, number of shards to use. Do not modify after usage, carefully plan ahead. */
    int shardCount() default -1;

    /** Partition key prefix to be used with sharding */
    String shardPrefix() default "shard";

    /** Sort keys to be compounded together */
    String[] rangeKeys() default {};

    /**
     * Prefix range key with this. If no range keys are present,
     * this will be the sole value of the range key.
     * Must be unique across instance of DynamoDB table.
     */
    String rangePrefix();

    @Target(TYPE)
    @Retention(RUNTIME)
    @interface DynamoTables {
        DynamoTable[] value();
    }
}
