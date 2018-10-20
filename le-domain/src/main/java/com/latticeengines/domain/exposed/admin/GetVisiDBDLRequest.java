package com.latticeengines.domain.exposed.admin;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GetVisiDBDLRequest {

    private String tenantName;

    public GetVisiDBDLRequest(String tenantName) {
        this.tenantName = tenantName;
    }

    @JsonProperty("tenantName")
    public String getTenantName() {
        return tenantName;
    }

    @JsonProperty("tenantName")
    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }
}
