package com.latticeengines.domain.exposed.auth;

import com.latticeengines.common.exposed.util.JsonUtils;

import io.swagger.annotations.ApiModel;

@ApiModel("Represents GlobalUserConfig with PIVOT representation of User level configuration")
public class GlobalAuthUserConfigSummary {

    private String tenantDeploymentId, ssoEnabled, forceSsoLogin;

    public GlobalAuthUserConfigSummary(String tenantDeploymentId, String ssoEnabled, String forceSsoLogin) {
        this.tenantDeploymentId = tenantDeploymentId;
        this.ssoEnabled = ssoEnabled;
        this.forceSsoLogin = forceSsoLogin;
    }

    public String getTenantDeploymentId() {
        return tenantDeploymentId;
    }

    public Boolean getSsoEnabled() {
        return Boolean.valueOf(ssoEnabled);
    }

    public Boolean getForceSsoLogin() {
        return Boolean.valueOf(forceSsoLogin);
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}