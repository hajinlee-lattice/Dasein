package com.latticeengines.skald.model;

public class FieldSchema {
    public FieldSchema(String name, FieldSource source, FieldType type, FieldInterpretation interpretation) {
        this.name = name;
        this.source = source;
        this.type = type;
        this.interpretation = interpretation;
    }

    // Serialization constructor.
    public FieldSchema() {
    }

    // The internal name of the column associated with this field.
    // Not intended for user-facing display except as a last resort.
    public String name;

    // Where the data for this field can be found.
    public FieldSource source;

    // The storage type and interpretation of the data in this field.
    public FieldType type;

    // What purpose this field has in the model.
    public FieldInterpretation interpretation;
}
