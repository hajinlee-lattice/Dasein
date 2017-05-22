package com.latticeengines.domain.exposed.metadata.statistics;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AttributeStatistics {
    @JsonProperty("Buckets")
    private List<Bucket> buckets = new ArrayList<>();

    public List<Bucket> getBuckets() {
        return buckets;
    }

    public void setBuckets(List<Bucket> buckets) {
        this.buckets = buckets;
    }
}
