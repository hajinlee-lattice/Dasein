package com.latticeengines.domain.exposed.skald.model;

public enum FieldType {
    BOOLEAN(Boolean.class),

    INTEGER(Long.class),

    // Typical stored as a double precision value.
    FLOAT(Double.class),

    // Encoded with UTF-8.
    STRING(String.class),

    // Milliseconds since the unix epoch stored in an int64.
    TEMPORAL(Long.class);

    private FieldType(Class<?> type) {
        this.type = type;
    }

    private final Class<?> type;

    public Class<?> type() {
        return type;
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
            case TEMPORAL:
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