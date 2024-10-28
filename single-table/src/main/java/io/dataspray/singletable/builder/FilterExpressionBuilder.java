package io.dataspray.singletable.builder;

public interface FilterExpressionBuilder<P> extends ConditionAndFilterExpressionBuilder<P> {

    P filterExpression(String expression);

    P filterExpression(MappingExpression mappingExpression);
}
