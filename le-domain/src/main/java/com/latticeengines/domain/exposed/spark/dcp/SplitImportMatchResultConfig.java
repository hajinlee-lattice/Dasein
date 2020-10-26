package com.latticeengines.domain.exposed.spark.dcp;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class SplitImportMatchResultConfig extends SparkJobConfig {

    public static final String NAME = "splitImportMatchResult";

    @JsonProperty("AcceptedAttrs")
    private List<String> acceptedAttrs;

    @JsonProperty("RejectedAttrs")
    private List<String> rejectedAttrs;

    @JsonProperty("DisplayNameMap")
    private Map<String, String> displayNameMap;

    @JsonProperty("MatchedDunsAttr")
    private String matchedDunsAttr;

    @JsonProperty("ClassificationAttr")
    private String classificationAttr;

    @JsonProperty("CountryAttr")
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

    public List<String> getAcceptedAttrs() {
        return acceptedAttrs;
    }

    public void setAcceptedAttrs(List<String> acceptedAttrs) {
        this.acceptedAttrs = acceptedAttrs;
    }

    public List<String> getRejectedAttrs() {
        return rejectedAttrs;
    }

    public void setRejectedAttrs(List<String> rejectedAttrs) {
        this.rejectedAttrs = rejectedAttrs;
    }

    public Map<String, String> getDisplayNameMap() {
        return displayNameMap;
    }

    public void setDisplayNameMap(Map<String, String> displayNameMap) {
        this.displayNameMap = displayNameMap;
    }

    public String getMatchedDunsAttr() {
        return matchedDunsAttr;
    }

    public void setMatchedDunsAttr(String matchedDunsAttr) {
        this.matchedDunsAttr = matchedDunsAttr;
    }

    public String getClassificationAttr() {
        return classificationAttr;
    }

    public void setClassificationAttr(String classificationAttr) {
        this.classificationAttr = classificationAttr;
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
