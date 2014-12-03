package com.latticeengines.domain.exposed.skald.model;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

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
        return new HashCodeBuilder(123, 31).append(source).append(type).append(interpretation).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
