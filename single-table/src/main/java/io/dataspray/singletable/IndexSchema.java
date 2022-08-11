// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.amazonaws.services.dynamodbv2.document.Index;

public interface IndexSchema<T> extends Schema<T> {
    Index index();

    String indexName();
}
