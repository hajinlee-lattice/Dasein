package com.latticeengines.domain.exposed.query.frontend;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BucketRestriction;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FrontEndRestriction {
    @JsonProperty("any")
    private List<BucketRestriction> any = new ArrayList<>();

    @JsonProperty("all")
    private List<BucketRestriction> all = new ArrayList<>();

    public List<BucketRestriction> getAny() {
        return any;
    }

    public void setAny(List<BucketRestriction> any) {
        this.any = any;
    }

    public List<BucketRestriction> getAll() {
        return all;
    }

    public void setAll(List<BucketRestriction> all) {
        this.all = all;
    }

}
