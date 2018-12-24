package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class CancelActionEmailInfo {

    @JsonProperty("actionName")
    private String actionName;
    @JsonProperty("tenantName")
    private String tenantName;
    @JsonProperty("actionUserName")
    private String actionUserName;


    public String getActionName() {
        return actionName;
    }

    public void setActionName(String actionName) {
        this.actionName = actionName;
    }

    public String getTenantName() {
        return tenantName;
    }

    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }

    public String getActionUserName() {
        return actionUserName;
    }

    public void setActionUserName(String actionUserName) {
        this.actionUserName = actionUserName;
    }

    public String toString() {
        return "actionName = " + this.actionName + "tenantName = " + this.tenantName + "actionUserName = " + this.actionUserName;
    }
}
