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

    // this property is used for testing
    @JsonProperty("tenantName")
    private String tenantName;

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

    public String getTenantName() {
        return tenantName;
    }

    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }
}
