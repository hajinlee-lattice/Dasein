package com.latticeengines.domain.exposed.datacloud.statistics;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AttributeStats implements Serializable {
    private static final long serialVersionUID = -561681993245086713L;

    @JsonProperty("Cnt")
    private Long nonNullCount;

    @JsonProperty("Bkts")
    private Buckets buckets;

    public AttributeStats() {
    }

    public AttributeStats(AttributeStats stats) {
        // used for deep copy during stats calculation
        this();
        this.nonNullCount = stats.nonNullCount;
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
