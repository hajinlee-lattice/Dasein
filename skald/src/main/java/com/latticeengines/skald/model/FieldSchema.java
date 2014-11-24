package com.latticeengines.skald.model;

public class FieldSchema {
    public FieldSchema(FieldSource source, FieldType type, FieldInterpretation interpretation) {
        this.source = source;
        this.type = type;
        this.interpretation = interpretation;
    }

    // Serialization constructor.
    public FieldSchema() {
    }

    // Where the data for this field can be found.
    public FieldSource source;

    // The storage type and interpretation of the data in this field.
    public FieldType type;

    // What purpose this field has in the model.
    public FieldInterpretation interpretation;
}
