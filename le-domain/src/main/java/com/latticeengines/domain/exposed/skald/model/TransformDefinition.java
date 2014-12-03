package com.latticeengines.domain.exposed.skald.model;

import java.util.Map;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

public class TransformDefinition {
    public TransformDefinition(String name, String output, FieldType type, Map<String, Object> arguments) {
        this.name = name;
        this.output = output;
        this.type = type;
        this.arguments = arguments;
    }

    // Serialization constructor.
    public TransformDefinition() {
    }

    // The name of the transform file to be invoked.
    public String name;

    // The name of the field this transform will populate.
    public String output;

    // The output type of the transformation.
    public FieldType type;

    // Any arguments to the transformation function.
    public Map<String, Object> arguments;

    @Override
    public int hashCode() {
        return new HashCodeBuilder(5, 201).append(name).append(output).append(type).append(arguments).toHashCode();
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
