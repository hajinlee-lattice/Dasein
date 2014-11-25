package com.latticeengines.skald.model;

public enum FieldType {
    BOOLEAN(Boolean.class),

    INTEGER(Integer.class),

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
}