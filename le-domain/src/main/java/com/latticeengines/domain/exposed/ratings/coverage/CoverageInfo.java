package com.latticeengines.domain.exposed.ratings.coverage;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.BucketMetadata;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CoverageInfo {

    @JsonProperty("accountCount")
    private Long accountCount;

    @JsonProperty("contactCount")
    private Long contactCount;

    @JsonProperty("contactCountWithoutEmail")
    private Long contactCountWithoutEmail;

    @JsonProperty("unscoredAccountCount")
    private Long unscoredAccountCount;

    @JsonProperty("unscoredContactCount")
    private Long unscoredContactCount;

    @JsonProperty("bucketCoverageCounts")
    private List<RatingBucketCoverage> bucketCoverageCounts;

    public CoverageInfo() {}

    public CoverageInfo(Long accountCount, Long contactCount) {
        this.accountCount = accountCount;
        this.contactCount = contactCount;
    }

    public CoverageInfo(Long accountCount, Long contactCount, Map<String, Long> countsMap) {
        this(accountCount, contactCount);
        this.bucketCoverageCounts = fromCounts(countsMap);
    }

    public CoverageInfo(Long accountCount, Long contactCount, List<BucketMetadata> buckets) {
        this(accountCount, contactCount);
        this.bucketCoverageCounts = fromBuckets(buckets);
    }

    public static List<RatingBucketCoverage> fromCounts(Map<String, Long> countsMap) {
        if (MapUtils.isNotEmpty(countsMap)) {
            Map<String, Long> ratingCounts = new TreeMap<>(countsMap);
            return ratingCounts.entrySet().stream().map(entry -> {
                RatingBucketCoverage bktCvg = new RatingBucketCoverage();
                bktCvg.setBucket(entry.getKey());
                bktCvg.setCount(entry.getValue());
                return bktCvg;
            }).collect(Collectors.toList());
        }
        return null;
    }

    public static List<RatingBucketCoverage> fromBuckets(List<BucketMetadata> buckets) {
        if (CollectionUtils.isNotEmpty(buckets)) {
            return buckets.stream().sorted(Comparator.comparing(BucketMetadata::getBucketName)).map(bucket -> {
                RatingBucketCoverage bktCvg = new RatingBucketCoverage();
                bktCvg.setBucket(bucket.getBucket().toValue());
                bktCvg.setCount((long) bucket.getNumLeads());
                return bktCvg;
            }).collect(Collectors.toList());
        }
        return null;
    }

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

    public Long getContactCountWithoutEmail() {
        return contactCountWithoutEmail;
    }

    public void setContactCountWithoutEmail(Long contactCountWithoutEmail) {
        this.contactCountWithoutEmail = contactCountWithoutEmail;
    }

    public Long getUnscoredAccountCount() {
        return unscoredAccountCount;
    }

    public void setUnscoredAccountCount(Long unscoredAccountCount) {
        this.unscoredAccountCount = unscoredAccountCount;
    }

    public Long getUnscoredContactCount() {
        return unscoredContactCount;
    }

    public void setUnscoredContactCount(Long unscoredContactCount) {
        this.unscoredContactCount = unscoredContactCount;
    }

    public List<RatingBucketCoverage> getBucketCoverageCounts() {
        return bucketCoverageCounts;
    }

    public void setBucketCoverageCounts(List<RatingBucketCoverage> bucketCoverageCounts) {
        this.bucketCoverageCounts = bucketCoverageCounts;
    }

    public RatingBucketCoverage getCoverageForBucket(String bucket) {
        if (this.bucketCoverageCounts == null) {
            return null;
        }
        return this.bucketCoverageCounts.stream().filter(rbc -> rbc.getBucket().equalsIgnoreCase(bucket)).findFirst()
                .orElse(null);
    }
}
