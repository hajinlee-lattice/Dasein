package com.latticeengines.common.exposed.query;

import java.util.List;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Query {
    private List<Lookup> lookups;
    private Restriction restriction;

    public Query(List<Lookup> lookups, Restriction restriction) {
        this.lookups = lookups;
        this.restriction = restriction;
    }

    @JsonProperty("restriction")
    public Restriction getRestriction() {
        return restriction;
    }

    @JsonProperty("restriction")
    public void setRestriction(Restriction restriction) {
        this.restriction = restriction;
    }

    @JsonProperty("lookups")
    public List<Lookup> getLookups() {
        return lookups;
    }

    @JsonProperty("lookups")
    public void setLookups(List<Lookup> lookups) {
        this.lookups = lookups;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public Query() {
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
