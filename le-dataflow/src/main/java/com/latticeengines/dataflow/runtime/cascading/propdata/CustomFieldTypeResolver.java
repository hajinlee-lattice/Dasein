package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.lang.reflect.Type;

import cascading.scheme.util.FieldTypeResolver;

public class CustomFieldTypeResolver implements FieldTypeResolver {
    private static final long serialVersionUID = -8932956805127891993L;
    private CsvToAvroFieldMapping fieldMap;

    public CustomFieldTypeResolver(CsvToAvroFieldMapping fieldMap) {
        this.fieldMap = fieldMap;
    }

    @Override
    public String cleanField(int ordinal, String fieldName, Type type) {
        return (fieldMap.getAvroFieldName(fieldName) != null) ? fieldMap.getAvroFieldName(fieldName) : fieldName;
    }

    @Override
    public Type inferTypeFrom(int ordinal, String fieldName) {
        System.out.println(fieldName);
        return null;
    }

    @Override
    public String prepareField(int i, String fieldName, Type type) {
        return null;
    }

}
