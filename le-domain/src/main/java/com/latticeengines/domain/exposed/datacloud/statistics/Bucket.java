package com.latticeengines.domain.exposed.datacloud.statistics;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Bucket {
    @JsonProperty("Lbl")
    private String bucketLabel;

    @JsonProperty("Cnt")
    private Long count;

    @JsonProperty("En")
    @JsonInclude(Include.NON_NULL)
    private Long[] encodedCountList;

    public String getBucketLabel() {
        return bucketLabel;
    }

    public void setBucketLabel(String bucketLabel) {
        this.bucketLabel = bucketLabel;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long[] getEncodedCountList() {
        return encodedCountList;
    }

    public void setEncodedCountList(Long[] encodedCountList) {
        this.encodedCountList = encodedCountList;
    }
}
