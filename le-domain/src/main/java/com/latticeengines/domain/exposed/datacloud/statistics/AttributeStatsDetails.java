package com.latticeengines.domain.exposed.datacloud.statistics;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AttributeStatsDetails {
    @JsonProperty("Cnt")
    private Long nonNullCount;

    @JsonProperty("Bkts")
    private Buckets buckets;

    public AttributeStatsDetails() {
    }

    public AttributeStatsDetails(AttributeStatsDetails stats) {
        // used for deep copy during stats calculation
        this();
        this.nonNullCount = new Long(stats.nonNullCount);
        if (stats.buckets != null) {
            this.buckets = new Buckets(stats.buckets);
        }
    }

    public Long getNonNullCount() {
        return nonNullCount;
    }

    public void setNonNullCount(Long nonNullCount) {
        this.nonNullCount = nonNullCount;
    }

    public Buckets getBuckets() {
        return buckets;
    }

    public void setBuckets(Buckets buckets) {
        this.buckets = buckets;
    }
}
