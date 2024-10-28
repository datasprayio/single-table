package io.dataspray.singletable.builder;

import com.google.common.collect.ImmutableList;

public interface Mappings {

    String fieldMapping(String fieldName);

    String fieldMapping(ImmutableList<String> fieldPath);

    String fieldMapping(String fieldName, String fieldValue);

    String valueMapping(String fieldName, Object object);

    String constantMapping(String name, Object object);

    String constantMapping(ImmutableList<String> namePath, Object value);
}
