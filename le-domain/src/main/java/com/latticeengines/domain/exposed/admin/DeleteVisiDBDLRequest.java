package com.latticeengines.domain.exposed.admin;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DeleteVisiDBDLRequest {

    private String tenantName;
    private String deleteVisiDBOption;

    public DeleteVisiDBDLRequest(String tenantName, String deleteVisiDBOption){
        this.tenantName = tenantName;
        this.deleteVisiDBOption = deleteVisiDBOption;
    }

    @JsonProperty("tenantName")
    public String getTenantName() {
        return tenantName;
    }

    @JsonProperty("tenantName")
    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }

    @JsonProperty("deleteVisiDBOption")
    public String getDeleteVisiDBOption() {
        return deleteVisiDBOption;
    }

    @JsonProperty("deleteVisiDBOption")
    public void setDeleteVisiDBOption(String deleteVisiDBOption) {
        this.deleteVisiDBOption = deleteVisiDBOption;
    }
}
