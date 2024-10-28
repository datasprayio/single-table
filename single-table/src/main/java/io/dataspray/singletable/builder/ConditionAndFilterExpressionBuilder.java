package io.dataspray.singletable.builder;

public interface ConditionAndFilterExpressionBuilder<P> {

    P conditionExists();

    P conditionNotExists();

    P conditionFieldEquals(String fieldName, Object objectOther);

    P conditionFieldNotEquals(String fieldName, Object objectOther);

    P conditionFieldExists(String fieldName);

    P conditionFieldNotExists(String fieldName);
}
