package com.latticeengines.common.exposed.query;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Sort {
    @JsonProperty("lookups")
    private List<ColumnLookup> lookups;
    @JsonProperty("descending")
    private boolean descending;

    public Sort(List<ColumnLookup> lookups, boolean descending) {
        this.lookups = lookups;
        this.descending = descending;
    }

    public Sort(List<ColumnLookup> lookups) {
        this.lookups = lookups;
    }

    public Sort() {
        this(new ArrayList<ColumnLookup>());
    }

    public List<ColumnLookup> getLookups() {
        return lookups;
    }

    public void setLookups(List<ColumnLookup> lookups) {
        this.lookups = lookups;
    }

    public boolean getDescending() {
        return descending;
    }

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
