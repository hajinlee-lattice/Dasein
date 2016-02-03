package com.latticeengines.domain.exposed.scoringapi;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

public class BucketRange {
    // The user facing display name of this bucket.
    public String name;

    // Lower inclusive probability bound for this bucket.
    public Double lower;

    // Upper exclusive probability bound for this bucket.
    public Double upper;

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
