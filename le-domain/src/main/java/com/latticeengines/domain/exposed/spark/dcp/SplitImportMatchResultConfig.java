package com.latticeengines.domain.exposed.spark.dcp;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class SplitImportMatchResultConfig extends SparkJobConfig {

    public static final String NAME = "splitImportMatchResult";

    // (attr -> dispName) mapping for attributes in the accepted split
    @JsonProperty("AcceptedAttrs")
    private Map<String, String> acceptedAttrsMap;

    // (attr -> dispName) mapping for attributes in the rejected split
    @JsonProperty("RejectedAttrs")
    private Map<String, String> rejectedAttrsMap;

    @JsonProperty("MatchedDunsAttr")
    private String matchedDunsAttr;

    @JsonProperty("countryAttr")
    private String countryAttr;

    @JsonProperty("CountryCodeAttr")
    private String countryCodeAttr;

    @JsonProperty("ConfidenceCodeAttr")
    private String confidenceCodeAttr;

    @JsonProperty("TotalCount")
    private long totalCount;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return 2;
    }

    public Map<String, String> getAcceptedAttrsMap() {
        return acceptedAttrsMap;
    }

    public void setAcceptedAttrsMap(Map<String, String> acceptedAttrsMap) {
        this.acceptedAttrsMap = acceptedAttrsMap;
    }

    public Map<String, String> getRejectedAttrsMap() {
        return rejectedAttrsMap;
    }

    public void setRejectedAttrsMap(Map<String, String> rejectedAttrsMap) {
        this.rejectedAttrsMap = rejectedAttrsMap;
    }

    public String getMatchedDunsAttr() {
        return matchedDunsAttr;
    }

    public void setMatchedDunsAttr(String matchedDunsAttr) {
        this.matchedDunsAttr = matchedDunsAttr;
    }

    public String getCountryAttr() {
        return countryAttr;
    }

    public void setCountryAttr(String countryAttr) {
        this.countryAttr = countryAttr;
    }

    public String getCountryCodeAttr() {
        return countryCodeAttr;
    }

    public void setCountryCodeAttr(String countryCodeAttr) {
        this.countryCodeAttr = countryCodeAttr;
    }

    public String getConfidenceCodeAttr() {
        return confidenceCodeAttr;
    }

    public void setConfidenceCodeAttr(String confidenceCodeAttr) {
        this.confidenceCodeAttr = confidenceCodeAttr;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }
}
