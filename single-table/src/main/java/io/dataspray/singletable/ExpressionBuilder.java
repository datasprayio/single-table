// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import com.google.common.collect.*;
import io.dataspray.singletable.builder.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

@Slf4j
@RequiredArgsConstructor
public abstract class ExpressionBuilder<T, P, B> implements Mappings, UpdateExpressionBuilder<T, P>,
        ConditionExpressionBuilder<P>, FilterExpressionBuilder<P> {

    protected final Schema<T> schema;

    private final Map<String, String> nameMap = Maps.newHashMap();
    private final Map<String, AttributeValue> valMap = Maps.newHashMap();
    private final Map<String, String> setUpdates = Maps.newHashMap();
    private final Map<String, String> removeUpdates = Maps.newHashMap();
    private final Map<String, String> addUpdates = Maps.newHashMap();
    private final Map<String, String> deleteUpdates = Maps.newHashMap();
    private final List<String> conditionExpressions = Lists.newArrayList();
    private final List<Consumer<B>> builderAdjustments = Lists.newArrayList();

    abstract protected P getParent();

    public P builder(Consumer<B> builder) {
        builderAdjustments.add(builder);
        return getParent();
    }

    @Override
    public P conditionExpression(String expression) {
        conditionExpressions.add(expression);
        return getParent();
    }

    @Override
    public P conditionExpression(MappingExpression mappingExpression) {
        conditionExpression(mappingExpression.getExpression(this));
        return getParent();
    }

    @Override
    public P filterExpression(String expression) {
        conditionExpression(expression);
        return getParent();
    }

    @Override
    public P filterExpression(MappingExpression mappingExpression) {
        conditionExpression(mappingExpression);
        return getParent();
    }

    @Override
    public P conditionExists() {
        conditionExpressions.add("attribute_exists(" + fieldMapping(schema.partitionKeyName()) + ")");
        return getParent();
    }

    @Override
    public P conditionNotExists() {
        conditionExpressions.add("attribute_not_exists(" + fieldMapping(schema.partitionKeyName()) + ")");
        return getParent();
    }

    @Override
    public P conditionFieldEquals(String fieldName, Object objectOther) {
        conditionExpressions.add(fieldMapping(fieldName) + " = " + valueMapping(fieldName, objectOther));
        return getParent();
    }

    @Override
    public P conditionFieldNotEquals(String fieldName, Object objectOther) {
        conditionExpressions.add(fieldMapping(fieldName) + " <> " + valueMapping(fieldName, objectOther));
        return getParent();
    }

    @Override
    public P conditionFieldExists(String fieldName) {
        conditionExpressions.add("attribute_exists(" + fieldMapping(fieldName) + ")");
        return getParent();
    }

    @Override
    public P conditionFieldNotExists(String fieldName) {
        conditionExpressions.add("attribute_not_exists(" + fieldMapping(fieldName) + ")");
        return getParent();
    }

    @Override
    public P updateExpression(String expression) {
        setUpdates.put(expression, expression);
        return getParent();
    }

    @Override
    public P updateExpression(MappingExpression mappingExpression) {
        updateExpression(mappingExpression.getExpression(this));
        return getParent();
    }

    @Override
    public P upsert(T item) {
        upsert(item, ImmutableSet.of());
        return getParent();
    }

    @Override
    public P upsert(T item, ImmutableSet<String> skipFieldNames) {
        schema.toAttrMap(item).forEach((key, value) -> {
            if (schema.partitionKeyName().equals(key) || schema.rangeKeyName().equals(key)) {
                return;
            }
            if (skipFieldNames.contains(key)) {
                return;
            }
            set(key, value);
        });
        return getParent();
    }

    @Override
    public P set(String fieldName, Object object) {
        checkState(!setUpdates.containsKey(fieldName));
        setUpdates.put(fieldName,
                fieldMapping(fieldName) + " = " + valueMapping(fieldName, object));
        return getParent();
    }

    @Override
    public P set(ImmutableList<String> fieldPath, Object object) {
        checkArgument(!fieldPath.isEmpty());
        String fieldMapping = fieldMapping(fieldPath);
        checkState(!addUpdates.containsKey(fieldMapping));
        AttributeValue value = schema.toAttrValue(object);
        setUpdates.put(fieldMapping,
                fieldMapping + " = " + constantMapping(fieldPath, value));
        return getParent();
    }

    @Override
    public P setIncrement(String fieldName, Number increment) {
        checkState(!setUpdates.containsKey(fieldName));
        setUpdates.put(fieldName, String.format("%s = if_not_exists(%s, %s) + %s",
                fieldMapping(fieldName),
                fieldMapping(fieldName),
                constantMapping("zero", AttributeValue.fromN("0")),
                valueMapping(fieldName, increment)));
        return getParent();
    }

    @Override
    public P add(String fieldName, Object object) {
        checkState(!addUpdates.containsKey(fieldName));
        addUpdates.put(fieldName,
                fieldMapping(fieldName) + " " + valueMapping(fieldName, object));
        return getParent();
    }

    @Override
    public P add(ImmutableList<String> fieldPath, Object object) {
        checkArgument(!fieldPath.isEmpty());
        String fieldMapping = fieldMapping(fieldPath);
        checkState(!addUpdates.containsKey(fieldMapping));
        AttributeValue value = schema.toAttrValue(object);
        addUpdates.put(fieldMapping,
                fieldMapping + " " + constantMapping(fieldPath, value));
        return getParent();
    }

    @Override
    public P remove(String fieldName) {
        checkState(!removeUpdates.containsKey(fieldName));
        removeUpdates.put(fieldName, fieldMapping(fieldName));
        return getParent();
    }

    @Override
    public P remove(ImmutableList<String> fieldPath) {
        checkArgument(!fieldPath.isEmpty());
        String fieldMapping = fieldMapping(fieldPath);
        checkState(!addUpdates.containsKey(fieldMapping));
        removeUpdates.put(fieldMapping, fieldMapping);
        return getParent();
    }

    @Override
    public P delete(String fieldName, Object object) {
        checkState(!deleteUpdates.containsKey(fieldName));
        deleteUpdates.put(fieldName,
                fieldMapping(fieldName) + " " + valueMapping(fieldName, object));
        return getParent();
    }

    @Override
    public String fieldMapping(String fieldName) {
        String mappedName = "#" + sanitizeFieldMapping(fieldName);
        nameMap.put(mappedName, fieldName);
        return mappedName;
    }

    @Override
    public String fieldMapping(ImmutableList<String> fieldPath) {
        return fieldPath.stream()
                .map(this::fieldMapping)
                .collect(Collectors.joining("."));
    }

    @Override
    public String fieldMapping(String fieldName, String fieldValue) {
        String mappedName = "#" + sanitizeFieldMapping(fieldName);
        nameMap.put(mappedName, fieldValue);
        return mappedName;
    }

    @Override
    public String valueMapping(String fieldName, Object object) {
        return constantMapping(fieldName, object);
    }

    @Override
    public String constantMapping(String name, Object object) {
        String mappedName = ":" + sanitizeFieldMapping(name);
        AttributeValue value = schema.toAttrValue(object);
        valMap.put(mappedName, value);
        return mappedName;
    }

    @Override
    public String constantMapping(ImmutableList<String> namePath, Object value) {
        return constantMapping(namePath.stream()
                .map(String::toLowerCase)
                .collect(Collectors.joining("X")), value);
    }

    protected Expression<B> buildExpression() {
        ArrayList<String> updates = Lists.newArrayList();
        if (!setUpdates.isEmpty()) {
            updates.add("SET " + String.join(", ", setUpdates.values()));
        }
        if (!addUpdates.isEmpty()) {
            updates.add("ADD " + String.join(", ", addUpdates.values()));
        }
        if (!removeUpdates.isEmpty()) {
            updates.add("REMOVE " + String.join(", ", removeUpdates.values()));
        }
        if (!deleteUpdates.isEmpty()) {
            updates.add("DELETE " + String.join(", ", deleteUpdates.values()));
        }
        final Optional<String> updateOpt = Optional.ofNullable(Strings.emptyToNull(String.join(" ", updates)));
        final Optional<String> conditionOpt = Optional.ofNullable(Strings.emptyToNull(String.join(" AND ", conditionExpressions)));
        final Optional<ImmutableMap<String, String>> nameImmutableMapOpt = nameMap.isEmpty() ? Optional.empty() : Optional.of(ImmutableMap.copyOf(nameMap));
        final Optional<ImmutableMap<String, AttributeValue>> valImmutableMapOpt = valMap.isEmpty() ? Optional.empty() : Optional.of(ImmutableMap.copyOf(valMap));
        final ImmutableList<Consumer<B>> immutableBuilderAdjustments = ImmutableList.copyOf(builderAdjustments);
        log.trace("Built dynamo expression: update {} condition {} nameMap {} valKeys {}",
                updateOpt, conditionOpt, nameImmutableMapOpt, valImmutableMapOpt.map(ImmutableMap::keySet));
        return new Expression() {

            @Override
            public Optional<String> updateExpression() {
                return updateOpt;
            }

            @Override
            public Optional<String> conditionExpression() {
                return conditionOpt;
            }

            @Override
            public Optional<String> filterExpression() {
                return conditionOpt;
            }

            @Override
            public Optional<ImmutableMap<String, String>> expressionAttributeNames() {
                return nameImmutableMapOpt;
            }

            @Override
            public Optional<ImmutableMap<String, AttributeValue>> expressionAttributeValues() {
                return valImmutableMapOpt;
            }

            @Override
            public ImmutableList<Consumer<B>> builderAdjustments() {
                return immutableBuilderAdjustments;
            }

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(this)
                        .add("updateExpression", this.updateExpression())
                        .add("conditionExpression", this.conditionExpression())
                        .add("nameMap", this.expressionAttributeNames())
                        .add("valMap", this.expressionAttributeValues())
                        .toString();
            }
        };
    }

    private String sanitizeFieldMapping(String fieldName) {
        return fieldName.replaceAll("(^[^a-z])|[^a-zA-Z0-9]", "x");
    }

    @Override
    public String toString() {
        return buildExpression().toString();
    }
}
