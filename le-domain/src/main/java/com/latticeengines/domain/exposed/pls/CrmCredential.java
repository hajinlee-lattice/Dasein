package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class CrmCredential {

    private String userName;
    private String password;
    private String securityToken;
    private String url;
    private String company;

    private String orgId;

    public CrmCredential() {
    }

    public CrmCredential(CrmCredential crmCredential) {
        this.userName = crmCredential.userName;
        this.password = crmCredential.password;
        this.securityToken = crmCredential.securityToken;
        this.orgId = crmCredential.orgId;
        this.url = crmCredential.url;
        this.company = crmCredential.company;
    }

    @JsonProperty("UserName")
    public String getUserName() {
        return userName;
    }

    @JsonProperty("UserName")
    public void setUserName(String userName) {
        this.userName = userName;
    }

    @JsonProperty("Password")
    public String getPassword() {
        return password;
    }

    @JsonProperty("Password")
    public void setPassword(String password) {
        this.password = password;
    }

    @JsonProperty("SecurityToken")
    public String getSecurityToken() {
        return securityToken;
    }

    @JsonProperty("SecurityToken")
    public void setSecurityToken(String securityToken) {
        this.securityToken = securityToken;
    }

    @JsonProperty("Url")
    public String getUrl() {
        return url;
    }

    @JsonProperty("Url")
    public void setUrl(String url) {
        this.url = url;
    }

    @JsonProperty("Company")
    public String getCompany() {
        return company;
    }

    @JsonProperty("Company")
    public void setCompany(String company) {
        this.company = company;
    }

    @JsonProperty("OrgId")
    public String getOrgId() {
        return orgId;
    }

    @JsonProperty("OrgId")
    public void setOrgId(String orgId) {
        this.orgId = orgId;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
