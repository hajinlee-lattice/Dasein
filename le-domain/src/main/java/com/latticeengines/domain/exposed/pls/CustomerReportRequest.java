package com.latticeengines.domain.exposed.pls;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.customer.CustomerReportType;

public abstract class CustomerReportRequest {

    @JsonProperty("Type")
    private CustomerReportType type;

    @JsonProperty("ReportedByEmail")
    private String reportedByEmail;

    @JsonProperty("InputKeys")
    private Map<String, String> inputKeys;

    @JsonProperty("MatchedKeys")
    private Map<String, String> matchedKeys;


    public CustomerReportType getType() {
        return type;
    }

    public void setType(CustomerReportType type) {
        this.type = type;
    }

    public String getReportedByEmail() {
        return reportedByEmail;
    }

    public void setReportedByEmail(String reportedByEmail) {
        this.reportedByEmail = reportedByEmail;
    }

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
}

