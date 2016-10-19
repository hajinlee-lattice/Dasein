package com.latticeengines.domain.exposed.scoringapi;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

public class EnrichResponseMetadata extends BaseResponse {

    @JsonProperty("requestId")
    @ApiModelProperty(value = "The unique id associated to the request", required = true)
    private String requestId;

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }
}
