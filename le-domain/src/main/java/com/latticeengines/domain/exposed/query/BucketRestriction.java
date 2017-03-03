package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BucketRestriction extends Restriction {
    @JsonProperty("lhs")
    private ColumnLookup lhs;

    @JsonProperty("value")
    private int value;

    public BucketRestriction(ColumnLookup lhs, int value) {
        this.lhs = lhs;
        this.value = value;
    }

    public BucketRestriction() {
    }

    public ColumnLookup getLhs() {
        return lhs;
    }

    public void setLhs(ColumnLookup lhs) {
        this.lhs = lhs;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

}
