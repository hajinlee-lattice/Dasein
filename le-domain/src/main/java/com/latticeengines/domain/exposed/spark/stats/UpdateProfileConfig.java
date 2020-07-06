package com.latticeengines.domain.exposed.spark.stats;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class UpdateProfileConfig extends SparkJobConfig {

    public static final String NAME = "updateProfile";

    @JsonProperty("MaxCat")
    private int maxCat = 2048; // Maximum allowed category number

    @JsonProperty("MaxCatLen")
    private int maxCatLength = 1024; // Maximum allowed category attribute
    // length. If exceeded, this attribute is
    // not segmentable

    @JsonProperty("MaxDiscrete")
    private int maxDiscrete = 5; // Maximum allowed discrete bucket number

    @JsonProperty("IncludeAttrs")
    private List<String> includeAttrs;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public int getMaxCat() {
        return maxCat;
    }

    public void setMaxCat(int maxCat) {
        this.maxCat = maxCat;
    }

    public int getMaxCatLength() {
        return maxCatLength;
    }

    public void setMaxCatLength(int maxCatLength) {
        this.maxCatLength = maxCatLength;
    }

    public int getMaxDiscrete() {
        return maxDiscrete;
    }

    public void setMaxDiscrete(int maxDiscrete) {
        this.maxDiscrete = maxDiscrete;
    }

    public List<String> getIncludeAttrs() {
        return includeAttrs;
    }

    public void setIncludeAttrs(List<String> includeAttrs) {
        this.includeAttrs = includeAttrs;
    }
}
