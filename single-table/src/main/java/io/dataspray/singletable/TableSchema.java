// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import io.dataspray.singletable.builder.DeleteBuilder;
import io.dataspray.singletable.builder.GetBuilder;
import io.dataspray.singletable.builder.PutBuilder;
import io.dataspray.singletable.builder.UpdateBuilder;

public interface TableSchema<T> extends Schema<T> {

    GetBuilder<T> get();

    PutBuilder<T> put();

    DeleteBuilder<T> delete();

    UpdateBuilder<T> update();
}
