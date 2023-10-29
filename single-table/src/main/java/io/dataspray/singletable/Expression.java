// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.util.Optional;

public interface Expression {
    UpdateItemRequest.Builder toUpdateItemRequestBuilder();

    UpdateItemRequest.Builder toUpdateItemRequestBuilder(UpdateItemRequest.Builder builder);

    Optional<String> updateExpression();

    Optional<String> conditionExpression();

    Optional<ImmutableMap<String, String>> expressionAttributeNames();

    Optional<ImmutableMap<String, AttributeValue>> expressionAttributeValues();

    @Override
    String toString();
}
