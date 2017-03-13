package com.latticeengines.domain.exposed.datacloud.statistics;

import java.util.Random;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BucketRange;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Bucket {
    @JsonProperty("Lbl")
    private String bucketLabel;

    @JsonProperty("Cnt")
    private Long count;

    @JsonProperty("Id")
    private Long id;

    @JsonProperty("En")
    @JsonInclude(Include.NON_NULL)
    private Long[] encodedCountList;

    @JsonProperty("Range")
    private BucketRange range;

    @JsonProperty("Lift")
    private Double lift;

    public Bucket() {
        // TODO TEMPORARY FOR M10
        lift = new Random().nextDouble() * 5.0;
    }

    public String getBucketLabel() {
        return bucketLabel;
    }

    public void setBucketLabel(String bucketLabel) {
        this.bucketLabel = bucketLabel;
        // TODO TEMPORARY FOR M10
        this.range = new BucketRange(bucketLabel, bucketLabel);
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long[] getEncodedCountList() {
        return encodedCountList;
    }

    public void setEncodedCountList(Long[] encodedCountList) {
        this.encodedCountList = encodedCountList;
    }

    public BucketRange getRange() {
        return range;
    }

    public void setRange(BucketRange range) {
        this.range = range;
    }

    public Double getLift() {
        return lift;
    }

    public void setLift(Double lift) {
        this.lift = lift;
    }
}
