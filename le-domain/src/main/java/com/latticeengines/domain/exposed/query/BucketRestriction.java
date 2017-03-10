package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BucketRestriction extends Restriction {
    @JsonProperty("lhs")
    private ColumnLookup lhs;

    @JsonProperty("bucket")
    private Bucket bucket;

    public BucketRestriction(ColumnLookup lhs, Bucket bucket) {
        this.lhs = lhs;
        this.bucket = bucket;
    }

    public ColumnLookup getLhs() {
        return lhs;
    }

    public void setLhs(ColumnLookup lhs) {
        this.lhs = lhs;
    }

    public Bucket getBucket() {
        return bucket;
    }

    public void setBucket(Bucket bucket) {
        this.bucket = bucket;
    }
}
