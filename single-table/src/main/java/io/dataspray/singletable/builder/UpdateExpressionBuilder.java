package io.dataspray.singletable.builder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public interface UpdateExpressionBuilder<T, P> {

    P updateExpression(String expression);

    P updateExpression(MappingExpression mappingExpression);

    P upsert(T item);

    P upsert(T item, ImmutableSet<String> skipFieldNames);

    P set(String fieldName, Object object);

    P set(ImmutableList<String> fieldPath, Object object);

    P setIncrement(String fieldName, Number increment);

    P add(String fieldName, Object object);

    P add(ImmutableList<String> fieldPath, Object object);

    P remove(String fieldName);

    P remove(ImmutableList<String> fieldPath);

    P delete(String fieldName, Object object);
}
