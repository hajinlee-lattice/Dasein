package com.latticeengines.domain.exposed.scoringapi;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

public class TargetedModel {
    public TargetedModel(FilterDefinition filter, String model) {
        this.filter = filter;
        this.model = model;
    }

    // Serialization constructor.
    public TargetedModel() {
    }

    // Target filter that applies to this model.
    public FilterDefinition filter;

    // Model name, not yet bound to a version.
    public String model;

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
