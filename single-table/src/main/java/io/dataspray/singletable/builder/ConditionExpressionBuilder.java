package io.dataspray.singletable.builder;

public interface ConditionExpressionBuilder<P> extends ConditionAndFilterExpressionBuilder<P> {

    P conditionExpression(String expression);

    P conditionExpression(MappingExpression mappingExpression);

}
