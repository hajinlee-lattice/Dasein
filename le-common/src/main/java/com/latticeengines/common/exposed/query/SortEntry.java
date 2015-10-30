package com.latticeengines.common.exposed.query;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SortEntry {

    private SingleReferenceLookup lookup;
    private boolean ascending;

    public SortEntry(SingleReferenceLookup lookup, boolean ascending) {
        this.lookup = lookup;
        this.ascending = ascending;
    }

    @JsonProperty("lookup")
    public SingleReferenceLookup getLookup() {
        return lookup;
    }

    @JsonProperty("lookup")
    public void setLookup(SingleReferenceLookup lookup) {
        this.lookup = lookup;
    }

    @JsonProperty("ascending")
    public boolean getAscending() {
        return ascending;
    }

    @JsonProperty("ascending")
    public void setAscending(boolean ascending) {
        this.ascending = ascending;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public SortEntry() {
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
