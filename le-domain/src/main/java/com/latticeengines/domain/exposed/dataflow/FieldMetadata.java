package com.latticeengines.domain.exposed.dataflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.common.exposed.util.AvroUtils;

public class FieldMetadata {
    private Schema.Type avroType;
    private Class<?> javaType;
    private String fieldName;
    private Schema.Field avroField;
    private Map<String, String> properties = new HashMap<>();
    private boolean nullable = true;
    private List<FieldMetadata> ancestors = new ArrayList<>();
    private String tableName;
    private Schema listElementSchema;

    public FieldMetadata(FieldMetadata fm) {
        this(fm.getAvroType(), fm.javaType, fm.getFieldName(), fm.getField(), fm.getProperties(), null);
    }

    public FieldMetadata(String fieldName, Class<?> javaType) {
        this(AvroUtils.getAvroType(javaType), javaType, fieldName, null, null);
    }

    public FieldMetadata(String fieldName, Class<?> javaType, Map<String, String> properties) {
        this(AvroUtils.getAvroType(javaType), javaType, fieldName, null, null);
        properties.putAll(properties);
    }

    public FieldMetadata(String fieldName, Class<?> javaType, Schema avroSchema) {
        this(AvroUtils.getAvroType(javaType), javaType, fieldName, null, avroSchema);
        properties.putAll(properties);
    }
    
    public FieldMetadata(Schema.Type avroType, Class<?> javaType, String fieldName, Schema.Field avroField) {
        this(avroType, javaType, fieldName, avroField, null);
    }

    @SuppressWarnings("deprecation")
    public FieldMetadata(Schema.Type avroType, Class<?> javaType, String fieldName, Schema.Field avroField, Schema listElementSchema) {
        this.avroType = avroType;
        this.javaType = javaType;
        this.fieldName = fieldName;
        this.avroField = avroField;
        this.listElementSchema = listElementSchema;

        if (avroField != null) {
            properties.putAll(avroField.props());
        }
    }

    public FieldMetadata(Schema.Type avroType, Class<?> javaType, String fieldName, Schema.Field avroField,
            Map<String, String> properties, Schema avroSchema) {
        this(avroType, javaType, fieldName, avroField, avroSchema);
        if (properties != null) {
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

    public void addAncestors(List<FieldMetadata> ancestors) {
        this.ancestors.addAll(ancestors);
        eliminateDuplicateAncestors();
    }

    public void addAncestor(FieldMetadata field) {
        ancestors.add(field);
        eliminateDuplicateAncestors();
    }

    public List<FieldMetadata> getImmediateAncestors() {
        return ancestors;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return fieldName;
    }

    private void eliminateDuplicateAncestors() {
        Set<Pair<String, String>> set = new HashSet<>();

        ListIterator<FieldMetadata> iter = ancestors.listIterator();
        while (iter.hasNext()) {
            FieldMetadata metadata = iter.next();
            if (metadata.getTableName() != null) {
                Pair<String, String> pair = new ImmutablePair<>(metadata.getFieldName(), metadata.getTableName());
                if (set.contains(pair)) {
                    iter.remove();
                }
            }
        }
    }

    public Schema getListElementSchema() {
        return listElementSchema;
    }

    public void setListElementSchema(Schema listElementSchema) {
        this.listElementSchema = listElementSchema;
    }
}