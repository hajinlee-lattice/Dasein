package com.latticeengines.domain.exposed.datacloud.statistics;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AttributeStatsDetails {
    private int nonNullCount;

    private Buckets buckets;

    public int getNonNullCount() {
        return nonNullCount;
    }

    public void setNonNullCount(int nonNullCount) {
        this.nonNullCount = nonNullCount;
    }

    public Buckets getBuckets() {
        return buckets;
    }

    public void setBuckets(Buckets buckets) {
        this.buckets = buckets;
    }
}
