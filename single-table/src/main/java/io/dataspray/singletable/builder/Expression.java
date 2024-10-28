// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable.builder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Optional;
import java.util.function.Consumer;

public interface Expression<B> {

    Optional<String> updateExpression();

    Optional<String> conditionExpression();

    Optional<String> filterExpression();

    Optional<ImmutableMap<String, String>> expressionAttributeNames();

    Optional<ImmutableMap<String, AttributeValue>> expressionAttributeValues();

    ImmutableList<Consumer<B>> builderAdjustments();

    @Override
    String toString();
}
