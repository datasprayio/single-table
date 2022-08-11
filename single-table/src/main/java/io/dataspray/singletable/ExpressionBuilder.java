// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.google.common.collect.ImmutableList;

/**
 * Update expression builder that ensures updated pk or sk keys are properly
 * mapped.
 */
public interface ExpressionBuilder {
    ExpressionBuilder set(String fieldName, Object object);

    ExpressionBuilder set(ImmutableList<String> fieldPath, Object object);

    ExpressionBuilder setIncrement(String fieldName, Number increment);

    ExpressionBuilder setExpression(String fieldName, String valueExpression);

    ExpressionBuilder setExpression(String expression);

    ExpressionBuilder add(String fieldName, Object object);

    ExpressionBuilder add(ImmutableList<String> fieldPath, Object object);

    ExpressionBuilder remove(String fieldName);

    ExpressionBuilder remove(ImmutableList<String> fieldPath);

    ExpressionBuilder delete(String fieldName, Object object);


    String fieldMapping(String fieldName);

    String fieldMapping(ImmutableList<String> fieldPath);

    String fieldMapping(String fieldName, String fieldValue);

    String valueMapping(String fieldName, Object object);

    String constantMapping(String fieldName, Object object);

    String valueMapping(ImmutableList<String> fieldPath, Object object);


    ExpressionBuilder condition(String expression);

    ExpressionBuilder conditionExists();

    ExpressionBuilder conditionNotExists();

    ExpressionBuilder conditionFieldEquals(String fieldName, Object objectOther);

    ExpressionBuilder conditionFieldExists(String fieldName);

    ExpressionBuilder conditionFieldNotExists(String fieldName);


    Expression build();
}
