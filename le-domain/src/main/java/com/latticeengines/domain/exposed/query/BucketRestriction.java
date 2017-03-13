package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BucketRestriction extends Restriction {
    @JsonProperty("lhs")
    private ColumnLookup lhs;

    @JsonProperty("bucket")
    private BucketRange bucket;

    public BucketRestriction(ColumnLookup lhs, BucketRange bucket) {
        this.lhs = lhs;
        this.bucket = bucket;
    }

    public BucketRestriction() {
    }

    public ColumnLookup getLhs() {
        return lhs;
    }

    public void setLhs(ColumnLookup lhs) {
        this.lhs = lhs;
    }

    public BucketRange getBucket() {
        return bucket;
    }

    public void setBucket(BucketRange bucket) {
        this.bucket = bucket;
    }
}
