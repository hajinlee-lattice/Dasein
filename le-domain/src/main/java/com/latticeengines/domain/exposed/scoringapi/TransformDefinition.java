package com.latticeengines.domain.exposed.scoringapi;

import java.io.Serializable;
import java.util.Map;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.transform.TransformationMetadata;

public class TransformDefinition implements Serializable {

    private static final long serialVersionUID = 1812638246579109675L;

    // The name of the transform file to be invoked.
    @JsonProperty
    public String name;

    @JsonProperty
    // The name of the field this transform will populate.
    public String output;

    @JsonProperty
    // The output type of the transformation.
    public FieldType type;

    @JsonProperty
    // Any arguments to the transformation function.
    public Map<String, Object> arguments;

    @JsonIgnore
    public String outputDisplayName;

    @JsonIgnore
    public TransformationMetadata transformationMetadata;

    public TransformDefinition(String name, String output, FieldType type, Map<String, Object> arguments) {
        this.name = name;
        this.output = output;
        this.type = type;
        this.arguments = arguments;
    }

    // Serialization constructor.
    public TransformDefinition() {
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
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
