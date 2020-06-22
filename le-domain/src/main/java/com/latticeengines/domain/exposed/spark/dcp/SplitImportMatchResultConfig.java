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

    @JsonProperty("MatchedCountryAttr")
    private String matchedCountryAttr;

    @JsonProperty("CountryCode")
    private String countryCode;

    @JsonProperty("ConfidenceCode")
    private String confidenceCode;

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

    public String getMatchedCountryAttr() {
        return matchedCountryAttr;
    }

    public void setMatchedCountryAttr(String matchedCountryAttr) {
        this.matchedCountryAttr = matchedCountryAttr;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    public String getConfidenceCode() {
        return confidenceCode;
    }

    public void setConfidenceCode(String confidenceCode) {
        this.confidenceCode = confidenceCode;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }
}
