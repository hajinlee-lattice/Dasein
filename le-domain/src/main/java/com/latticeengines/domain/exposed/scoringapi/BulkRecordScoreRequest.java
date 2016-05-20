package com.latticeengines.domain.exposed.scoringapi;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

public class BulkRecordScoreRequest {
    @JsonProperty("source")
    @ApiModelProperty(value = "Name of the source system that originated this score request.")
    private String source;

    @JsonProperty("rule")
    @ApiModelProperty(value = "Name of the rule that initiated this score request")
    private String rule;

    @JsonProperty("records")
    @ApiModelProperty(value = "List of records", required = true)
    private List<Record> records;

    private String rootOperationId;

    private String requestTimestamp;

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getRule() {
        return rule;
    }

    public void setRule(String rule) {
        this.rule = rule;
    }

    public List<Record> getRecords() {
        return records;
    }

    public void setRecords(List<Record> records) {
        this.records = records;
    }

    public String getRootOperationId() {
        return rootOperationId;
    }

    public void setRootOperationId(String rootOperationId) {
        this.rootOperationId = rootOperationId;
    }

    public String getRequestTimestamp() {
        return requestTimestamp;
    }

    public void setRequestTimestamp(String requestTimestamp) {
        this.requestTimestamp = requestTimestamp;
    }

}
