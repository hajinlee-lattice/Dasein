package com.latticeengines.domain.exposed.datacloud.statistics;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Bucket {
    @JsonProperty("Lbl")
    private String bucketLabel;

    @JsonProperty("Cnt")
    private Integer count;

    public String getBucketLabel() {
        return bucketLabel;
    }

    public void setBucketLabel(String bucketLabel) {
        this.bucketLabel = bucketLabel;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
}
