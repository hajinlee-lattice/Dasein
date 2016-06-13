package com.latticeengines.domain.exposed.dataflow;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;

import com.latticeengines.common.exposed.util.AvroUtils;

public class FieldMetadata {
    private Schema.Type avroType;
    private Class<?> javaType;
    private String fieldName;
    private Schema.Field avroField;
    private Map<String, String> properties = new HashMap<>();
    private boolean nullable = true;

    public FieldMetadata(FieldMetadata fm) {
        this(fm.getAvroType(), fm.javaType, fm.getFieldName(), fm.getField(), fm.getProperties());
    }

    public FieldMetadata(String fieldName, Class<?> javaType) {
        this(AvroUtils.getAvroType(javaType), javaType, fieldName, null);
    }

    public FieldMetadata(String fieldName, Class<?> javaType, Map<String, String> properties) {
        this(AvroUtils.getAvroType(javaType), javaType, fieldName, null);
        properties.putAll(properties);
    }

    @SuppressWarnings("deprecation")
    public FieldMetadata(Schema.Type avroType, Class<?> javaType, String fieldName, Schema.Field avroField) {
        this.avroType = avroType;
        this.javaType = javaType;
        this.fieldName = fieldName;
        this.avroField = avroField;

        if (avroField != null) {
            properties.putAll(avroField.props());
        }
    }

    public FieldMetadata(Schema.Type avroType, Class<?> javaType, String fieldName, Schema.Field avroField,
            Map<String, String> properties) {
        this(avroType, javaType, fieldName, avroField);
        if (avroField == null && properties != null) {
            this.properties.putAll(properties);
        }
    }

    public Schema.Type getAvroType() {
        return avroType;
    }

    public Class<?> getJavaType() {
        return javaType;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Schema.Field getField() {
        return avroField;
    }

    public String getPropertyValue(String key) {
        return properties.get(key);
    }

    public void setPropertyValue(String key, String value) {
        properties.put(key, value);
    }

    public void addProperties(Map<String, String> properties) {
        this.properties.putAll(properties);
    }

    public Set<Map.Entry<String, String>> getEntries() {
        return properties.entrySet();
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    @Override
    public String toString() {
        return fieldName;
    }
}