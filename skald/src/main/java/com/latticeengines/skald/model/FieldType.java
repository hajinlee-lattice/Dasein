package com.latticeengines.skald.model;

public enum FieldType {
    Boolean,

    Integer,

    // Typical stored as a double precision value.
    Float,

    // Encoded with UTF-8.
    String,

    // Milliseconds since the unix epoch stored in an int64.
    Temporal
}
