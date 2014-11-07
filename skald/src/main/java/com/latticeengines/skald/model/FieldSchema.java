package com.latticeengines.skald.model;

public class FieldSchema {
    // The internal name of the column associated with this field.
    // Not intended for user-facing display except as a last resort.
    public String name;

    // Where the data for this field can be found.
    public FieldSource source;

    // The storage type and interpretation of the data in this field.
    public FieldType type;
}
