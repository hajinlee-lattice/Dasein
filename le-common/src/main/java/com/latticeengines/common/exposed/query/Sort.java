package com.latticeengines.common.exposed.query;

import java.util.List;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Sort {
    private List<SortEntry> entries;

    public Sort(List<SortEntry> entries) {
        this.entries = entries;
    }

    @JsonProperty("entries")
    public List<SortEntry> getEntries() {
        return entries;
    }

    @JsonProperty("entries")
    public void setEntries(List<SortEntry> entries) {
        this.entries = entries;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public Sort() {
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
