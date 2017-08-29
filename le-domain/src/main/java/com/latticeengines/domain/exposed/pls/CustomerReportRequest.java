package com.latticeengines.domain.exposed.pls;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class CustomerReportRequest {

    @JsonProperty("InputKeys")
    private Map<String, String> inputKeys;

    @JsonProperty("MatchedKeys")
    private Map<String, String> matchedKeys;

    @JsonProperty("MatchLog")
    private List<String> matchLog;

    @JsonProperty("CorrectValue")
    private String correctValue;

    @JsonProperty("Comment")
    private String comment;

    public Map<String, String> getInputKeys() {
        return inputKeys;
    }

    public void setInputKeys(Map<String, String> inputKeys) {
        this.inputKeys = inputKeys;
    }

    public Map<String, String> getMatchedKeys() {
        return matchedKeys;
    }

    public void setMatchedKeys(Map<String, String> matchedKeys) {
        this.matchedKeys = matchedKeys;
    }

    public String getCorrectValue() {
        return correctValue;
    }

    public void setCorrectValue(String correctValue) {
        this.correctValue = correctValue;
    }

    public List<String> getMatchLog() {
        return matchLog;
    }

    public void setMatchLog(List<String> matchLog) {
        this.matchLog = matchLog;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }
}

