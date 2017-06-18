package com.latticeengines.domain.exposed.datacloud.statistics;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.frontend.FrontEndBucket;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class Bucket implements Serializable {

    private static final long serialVersionUID = -8550825595883518157L;

    @JsonProperty("Lbl")
    private String bucketLabel;

    @JsonProperty("Cnt")
    private Long count;

    @JsonProperty("Id")
    private Long id;

    @JsonProperty("En")
    private Long[] encodedCountList;

    @JsonProperty("Bkt")
    private FrontEndBucket bkt;

    @JsonProperty("Lift")
    private Double lift;

    public Bucket() {
    }

    public Bucket(Bucket bucket) {
        // used for deep copy during stats calculation
        this();
        this.bucketLabel = bucket.bucketLabel;
        if (bucket.count != null) {
            this.count = new Long(bucket.count);
        }
        this.id = bucket.id;
        if (bucket.encodedCountList != null) {
            this.encodedCountList = new Long[bucket.encodedCountList.length];
            int idx = 0;
            for (Long cnt : bucket.encodedCountList) {
                this.encodedCountList[idx++] = new Long(cnt);
            }
        }
        if (bucket.bkt != null) {
            this.bkt = bucket.bkt;
        }
        if (bucket.lift != null) {
            this.lift = new Double(bucket.lift);
        }
    }

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

    public FrontEndBucket getBkt() {
        return bkt;
    }

    public void setBkt(FrontEndBucket bkt) {
        this.bkt = bkt;
    }

    public Double getLift() {
        return lift;
    }

    public void setLift(Double lift) {
        this.lift = lift;
    }

    public int getIdAsInt() {
        return id.intValue();
    }
}
