// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.amazonaws.services.dynamodbv2.document.Table;

public interface TableSchema<T> extends Schema<T> {
    String tableName();

    Table table();

    ExpressionBuilder expressionBuilder();
}
