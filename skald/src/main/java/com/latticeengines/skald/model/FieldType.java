package com.latticeengines.skald.model;

public enum FieldType {
    Boolean(Boolean.class),

    Integer(Integer.class),

    // Typical stored as a double precision value.
    Float(Double.class),

    // Encoded with UTF-8.
    String(String.class),

    // Milliseconds since the unix epoch stored in an int64.
    Temporal(Long.class);

    private FieldType(Class<?> type) {
        this.type = type;
    }

    private final Class<?> type;

    public Class<?> type() {
        return type;
    }
}