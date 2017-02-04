package com.latticeengines.domain.exposed.ulysses;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CompanyProfile {

    @JsonProperty("attributes")
    private Map<String, String> attributes = new HashMap<>();

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("matchErrorMessages")
    private List<String> matchErrorMessages = new ArrayList<>();

    @JsonProperty("matchLogs")
    private List<String> matchLogs = new ArrayList<>();

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
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
}
