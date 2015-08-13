package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.List;

public class Query {
    public Query(List<Lookup> lookups, Restriction restriction) {
        this.lookups = lookups;
        this.restriction = restriction;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public Query() {
    }

    @JsonProperty
    public List<Lookup> lookups;

    @JsonProperty
    public Restriction restriction;

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
