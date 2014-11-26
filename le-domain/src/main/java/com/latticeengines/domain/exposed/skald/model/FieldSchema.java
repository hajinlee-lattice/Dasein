package com.latticeengines.domain.exposed.skald.model;

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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((interpretation == null) ? 0 : interpretation.hashCode());
        result = prime * result + ((source == null) ? 0 : source.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        FieldSchema other = (FieldSchema) obj;
        if (interpretation != other.interpretation)
            return false;
        if (source != other.source)
            return false;
        if (type != other.type)
            return false;
        return true;
    }

    @Override
    public String toString() {
        // TODO
        return null;
    }
}
