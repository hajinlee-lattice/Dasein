package com.latticeengines.domain.exposed.ulysses;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CompanyProfile {

    @JsonProperty("attributes")
    private Map<String, Object> attributes = new HashMap<>();

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("matchErrorMessages")
    private List<String> matchErrorMessages = new ArrayList<>();

    @JsonProperty("matchLogs")
    private List<String> matchLogs = new ArrayList<>();

    @JsonProperty("companyInfo")
    private Map<String, Object> companyInfo;

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public List<String> getMatchErrorMessages() {
        return matchErrorMessages;
    }

    public void setMatchErrorMessages(List<String> matchErrorMessages) {
        this.matchErrorMessages = matchErrorMessages;
    }

    public List<String> getMatchLogs() {
        return matchLogs;
    }

    public void setMatchLogs(List<String> matchLogs) {
        this.matchLogs = matchLogs;
    }

    public Map<String, Object> getCompanyInfo() {
        return companyInfo;
    }

    public void setCompanyInfo(Map<String, Object> companyInfo) {
        this.companyInfo = companyInfo;
    }

}
