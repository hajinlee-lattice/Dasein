package com.latticeengines.domain.exposed.scoringapi;

import java.util.HashMap;
import java.util.Map;

public enum FieldType {
    BOOLEAN(Boolean.class, "boolean"), //
    INTEGER(Long.class, "int", "long"), //
    // Typical stored as a double precision value.
    FLOAT(Double.class, "float", "double"), //
    // Encoded with UTF-8.
    STRING(String.class, "string");

    private static Map<Class<?>, FieldType> javaToFieldTypeMap = new HashMap<>();
    private static Map<String, FieldType> avroToFieldTypeMap = new HashMap<>();

    static {
        for (FieldType fieldType : FieldType.values()) {
            javaToFieldTypeMap.put(fieldType.type(), fieldType);
            for (String avroType : fieldType.avroTypes) {
                avroToFieldTypeMap.put(avroType, fieldType);
            }
        }
    }

    private FieldType(Class<?> type, String... avroTypes) {
        this.type = type;
        this.avroTypes = avroTypes;
    }

    private final Class<?> type;
    private final String[] avroTypes;

    public Class<?> type() {
        return type;
    }
    
    public String[] avroTypes() {
        return avroTypes;
    }

    public static FieldType getFromJavaType(Class<?> javaType) {
        return javaToFieldTypeMap.get(javaType);
    }

    public static FieldType getFromAvroType(String avroType) {
        return avroToFieldTypeMap.get(avroType);
    }

    public static Object parse(FieldType fieldtype, Object rawvalue) {
        if (rawvalue == null) {
            return null;
        } else {
            return parse(fieldtype, String.valueOf(rawvalue));
        }
    }

    public static Object parse(FieldType fieldtype, String rawvalue) {
        if (rawvalue == null) {
            return null;
        }

        try {
            switch (fieldtype) {
            case BOOLEAN:
                if (rawvalue.equals("1") || rawvalue.equalsIgnoreCase("true")) {
                    return Boolean.TRUE;
                } else if (rawvalue.equals("0") || rawvalue.equalsIgnoreCase("false")) {
                    return Boolean.FALSE;
                } else {
                    throw new RuntimeException("Invalid value for BOOLEAN " + rawvalue);
                }
            case FLOAT:
                return Double.parseDouble(rawvalue);
            case INTEGER:
                return Long.parseLong(rawvalue);
            case STRING:
                return rawvalue;
            default:
                throw new UnsupportedOperationException("Unsupported field type " + fieldtype);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failure converting value " + rawvalue + " to FieldType " + fieldtype, e);
        }
    }

}