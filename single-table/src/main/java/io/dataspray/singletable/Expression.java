// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.google.common.collect.ImmutableMap;

import java.util.Optional;

public interface Expression {
    Optional<String> updateExpression();

    Optional<String> conditionExpression();

    Optional<ImmutableMap<String, String>> nameMap();

    Optional<ImmutableMap<String, Object>> valMap();

    @Override
    String toString();
}
