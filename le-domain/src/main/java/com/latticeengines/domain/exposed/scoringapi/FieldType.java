package com.latticeengines.domain.exposed.scoringapi;

import java.util.HashMap;
import java.util.Map;

import org.dmg.pmml.DataType;

public enum FieldType {
    BOOLEAN(Boolean.class, new DataType[] { DataType.BOOLEAN }, "boolean"), //
    INTEGER(Long.class, new DataType[] { DataType.INTEGER }, "int"), //
    // Typical stored as a double precision value.
    FLOAT(Double.class, new DataType[] { DataType.FLOAT, DataType.DOUBLE }, "float", "double"), //
    LONG(Long.class, new DataType[] { DataType.INTEGER }, "long"), //
    // Encoded with UTF-8.
    STRING(String.class, new DataType[] { DataType.STRING }, "string");

    private static Map<Class<?>, FieldType> javaToFieldTypeMap = new HashMap<>();
    private static Map<String, FieldType> avroToFieldTypeMap = new HashMap<>();
    private static Map<DataType, FieldType> pmmlTypeToFieldTypeMap = new HashMap<>();

    static {
        for (FieldType fieldType : FieldType.values()) {
            javaToFieldTypeMap.put(fieldType.type(), fieldType);

            for (DataType pmmlType : fieldType.pmmlTypes) {
                pmmlTypeToFieldTypeMap.put(pmmlType, fieldType);
            }

            for (String avroType : fieldType.avroTypes) {
                avroToFieldTypeMap.put(avroType, fieldType);
            }

        }
    }

    private final Class<?> type;
    private final String[] avroTypes;
    private final DataType[] pmmlTypes;

    FieldType(Class<?> type, DataType[] pmmlTypes, String... avroTypes) {
        this.type = type;
        this.pmmlTypes = pmmlTypes;
        this.avroTypes = avroTypes;
    }

    public static FieldType getFromJavaType(Class<?> javaType) {
        return javaToFieldTypeMap.get(javaType);
    }

    public static FieldType getFromAvroType(String avroType) {
        return avroToFieldTypeMap.get(avroType);
    }

    public static FieldType getFromPmmlType(DataType pmmlType) {
        return pmmlTypeToFieldTypeMap.get(pmmlType);
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
        } else if (!fieldtype.equals(FieldType.STRING)) {
            rawvalue = rawvalue.trim();
            if (rawvalue.isEmpty()) {
                return null;
            }
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
                case LONG:
                    return Long.parseLong(rawvalue);
                case STRING:
                    return rawvalue;
                default:
                    throw new UnsupportedOperationException("Unsupported field type " + fieldtype);
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failure converting value " + rawvalue + " to FieldType " + fieldtype, e);
        }
    }

    public Class<?> type() {
        return type;
    }

    public String[] avroTypes() {
        return avroTypes;
    }

    public DataType[] pmmlTypes() {
        return pmmlTypes;
    }
}
