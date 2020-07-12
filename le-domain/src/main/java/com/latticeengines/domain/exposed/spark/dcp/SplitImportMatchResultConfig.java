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

    @JsonProperty("ConfidenceCodeAttr")
    private String confidenceCodeAttr;

    @JsonProperty("TotalCount")
    private long totalCount;

    @JsonProperty("ManageDbUrl")
    private String manageDbUrl;

    @JsonProperty("User")
    private String user;

    @JsonProperty("Password")
    private String password;

    @JsonProperty("EncryptionKey")
    private String encryptionKey;

    @JsonProperty("SaltHint")
    private String saltHint;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return 3;
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

    public String getManageDbUrl() {
        return manageDbUrl;
    }

    public void setManageDbUrl(String manageDbUrl) {
        this.manageDbUrl = manageDbUrl;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getEncryptionKey() {
        return encryptionKey;
    }

    public void setEncryptionKey(String encryptionKey) {
        this.encryptionKey = encryptionKey;
    }

    public String getSaltHint() {
        return saltHint;
    }

    public void setSaltHint(String saltHint) {
        this.saltHint = saltHint;
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
