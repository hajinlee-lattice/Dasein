package com.latticeengines.domain.exposed.dcp.vbo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

@JsonInclude(Include.NON_NULL)
public class VboResponse {

    @JsonProperty("status")
    @ApiModelProperty(required = true, value = "status")
    private String status;

    @JsonProperty("message")
    @ApiModelProperty(required = true, value = "message")
    private String message;

    @JsonProperty("ackReferenceId")
    @ApiModelProperty(value = "ackReferenceId")
    private String ackReferenceId;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getAckReferenceId() {
        return ackReferenceId;
    }

    public void setAckReferenceId(String ackReferenceId) {
        this.ackReferenceId = ackReferenceId;
    }
}
