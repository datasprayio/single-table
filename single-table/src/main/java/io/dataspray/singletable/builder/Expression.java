// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable.builder;

import com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.Optional;

public interface Expression {

    Optional<String> updateExpression();

    Optional<String> conditionExpression();

    Optional<String> filterExpression();

    Optional<ImmutableMap<String, String>> expressionAttributeNames();

    Optional<ImmutableMap<String, AttributeValue>> expressionAttributeValues();

    @Override
    String toString();
}
