package com.latticeengines.domain.exposed.pls;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CoverageInfo {

    @JsonProperty("accountCount")
    private Long accountCount;

    @JsonProperty("contactCount")
    private Long contactCount;

    @JsonProperty("bucketCoverageCounts")
    private List<RatingBucketCoverage> bucketCoverageCounts;

    public Long getAccountCount() {
        return accountCount;
    }

    public void setAccountCount(Long accountCount) {
        this.accountCount = accountCount;
    }

    public Long getContactCount() {
        return contactCount;
    }

    public void setContactCount(Long contactCount) {
        this.contactCount = contactCount;
    }

    public List<RatingBucketCoverage> getBucketCoverageCounts() {
        return bucketCoverageCounts;
    }

    public void setBucketCoverageCounts(List<RatingBucketCoverage> bucketCoverageCounts) {
        this.bucketCoverageCounts = bucketCoverageCounts;
    }
}
