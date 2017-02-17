package com.latticeengines.domain.exposed.metadata.frontend.query;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FrontEndFilter {
    @JsonProperty("any")
    private List<FrontEndBucketRestriction> any = new ArrayList<>();

    @JsonProperty("all")
    private List<FrontEndBucketRestriction> all = new ArrayList<>();

    @JsonProperty("none")
    private List<FrontEndBucketRestriction> none = new ArrayList<>();

    public List<FrontEndBucketRestriction> getAny() {
        return any;
    }

    public void setAny(List<FrontEndBucketRestriction> any) {
        this.any = any;
    }

    public List<FrontEndBucketRestriction> getAll() {
        return all;
    }

    public void setAll(List<FrontEndBucketRestriction> all) {
        this.all = all;
    }

    public List<FrontEndBucketRestriction> getNone() {
        return none;
    }

    public void setNone(List<FrontEndBucketRestriction> none) {
        this.none = none;
    }
}
