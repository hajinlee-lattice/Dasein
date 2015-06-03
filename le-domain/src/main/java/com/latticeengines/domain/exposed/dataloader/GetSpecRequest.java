package com.latticeengines.domain.exposed.dataloader;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GetSpecRequest {

    private String tenantName;

    private String specName;

    public GetSpecRequest(String tenantName, String specName){
        this.tenantName = tenantName;
        this.specName = specName;
    }

    @JsonProperty("tenantName")
    public String getTenantName() {
        return tenantName;
    }

    @JsonProperty("tenantName")
    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }

    @JsonProperty("specName")
    public String getSpecName() {
        return specName;
    }

    @JsonProperty("specName")
    public void setSpecName(String specName) {
        this.specName = specName;
    }

}
