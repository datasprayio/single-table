// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import io.dataspray.singletable.builder.ShardedQueryBuilder;

public interface ShardedIndexSchema<T> extends Schema<T> {
    String indexName();

    ShardedQueryBuilder<T> querySharded();
}
