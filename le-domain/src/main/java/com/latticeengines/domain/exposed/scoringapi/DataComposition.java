package com.latticeengines.domain.exposed.scoringapi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

// Defines how to construct an input record for a PMML model.
public class DataComposition {
    public DataComposition(Map<String, FieldSchema> fields, List<TransformDefinition> transforms) {
        this.fields = fields;
        this.transforms = transforms;
    }

    // Serialization constructor.
    public DataComposition() {
        this.fields = new HashMap<String, FieldSchema>();
        this.transforms = new ArrayList<TransformDefinition>();
    }

    // All the fields from the event table that was used to create this model.
    // This schema should match the input provided to the modeling service.
    // Keyed by internal name of the column associated with this field.
    public Map<String, FieldSchema> fields;

    // The set of transforms to create new fields based on the existing ones.
    // Theses will be executed in order, which should capture any dependencies.
    public List<TransformDefinition> transforms;

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
