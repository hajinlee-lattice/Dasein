package com.latticeengines.domain.exposed.scoringapi;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

public class ScoreRequest {

    @JsonProperty("modelId")
    @ApiModelProperty(required = true)
    private String modelId;

    @JsonProperty("source")
    @ApiModelProperty(value = "Name of the source system that originated this score request.")
    private String source;

    @JsonProperty("rule")
    @ApiModelProperty(value = "Name of the rule that initiated this score request")
    private String rule;

    @JsonProperty("record")
    @ApiModelProperty(value = "A record is represented as a JSON Object; " //
            + "ie. { \"field1\" : value1, \"field2\" : value2, .......}. " //
            + "At the minimum, in case of lead type model, make sure to specify " //
            + "'Email' field and for non-lead type model specify either 'Website' " //
            + "or 'Domain' fields. If these fields are not specified then specify " //
            + "both 'CompanyName' and 'State' fields", //
            required = true)
    private Map<String, Object> record;

    public String getModelId() {
        return modelId;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public Map<String, Object> getRecord() {
        return record;
    }

    public void setRecord(Map<String, Object> record) {
        this.record = record;
    }

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

}
