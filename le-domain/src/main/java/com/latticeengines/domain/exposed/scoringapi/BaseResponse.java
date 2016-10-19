package com.latticeengines.domain.exposed.scoringapi;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

import io.swagger.annotations.ApiModelProperty;

public class BaseResponse {

    @JsonProperty("warnings")
    @ApiModelProperty(value = "Warnings")
    private List<Warning> warnings = new ArrayList<>();

    @JsonProperty("timestamp")
    @ApiModelProperty(value = "The UTC timestamp of this score in ISO8601 format", required = true)
    private String timestamp = "";

    public List<Warning> getWarnings() {
        return warnings;
    }

    public void setWarnings(List<Warning> warnings) {
        this.warnings = warnings;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String toString() {
        return JsonUtils.serialize(this);
    }
}
