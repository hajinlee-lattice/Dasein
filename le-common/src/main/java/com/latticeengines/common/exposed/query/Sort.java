package com.latticeengines.common.exposed.query;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Sort {
    private List<SingleReferenceLookup> lookups;
    private boolean descending;

    public Sort(List<SingleReferenceLookup> lookups, boolean descending) {
        this.lookups = lookups;
        this.descending = descending;
    }

    public Sort(List<SingleReferenceLookup> lookups) {
        this.lookups = lookups;
    }

    public Sort() {
        this(new ArrayList<SingleReferenceLookup>());
    }

    @JsonProperty("lookups")
    public List<SingleReferenceLookup> getLookups() {
        return lookups;
    }

    @JsonProperty("lookups")
    public void setLookups(List<SingleReferenceLookup> lookups) {
        this.lookups = lookups;
    }

    @JsonProperty("descending")
    public boolean getDescending() {
        return descending;
    }

    @JsonProperty("descending")
    public void setDescending(boolean descending) {
        this.descending = descending;
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
