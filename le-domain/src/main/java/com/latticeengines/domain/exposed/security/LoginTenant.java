package com.latticeengines.domain.exposed.security;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This sub-class of Tenant contains additional fields that are populated with data
 * from IDaaS.  It is intended to replace the Tenant objects in the LoginDocument.
 */
public class LoginTenant extends Tenant {

    @JsonProperty("CompanyName")
    private String companyName;

    @JsonProperty("DUNS")
    private String duns;

    @JsonProperty("SubscriptionType")
    private String subscriptionType;

    @JsonProperty("Country")
    private String country;

    @JsonProperty("ContractStartDate")
    private Date contractStartDate;

    @JsonProperty("ContractEndDate")
    private Date contractEndDate;

    public LoginTenant() {
    }

    /**
     * Copy the fields from the Tenant we are augmenting to create a LoginTenant.
     * @param tenant - The Tenant object to copy for use as the base for the LoginTenant
     */
    public LoginTenant(Tenant tenant) {
        this.setId(tenant.getId());
        this.setName(tenant.getName());
        this.setPid(tenant.getPid());
        this.setRegisteredTime(getRegisteredTime());
        this.setUiVersion(getUiVersion());
        this.setStatus(tenant.getStatus());
        this.setTenantType(getTenantType());
        this.setContract(getContract());
        this.setEntitledApps(getEntitledApps());
        this.setSubscriberNumber(getSubscriberNumber());
        this.setExpiredTime(getExpiredTime());
        this.setNotificationLevel(tenant.getNotificationLevel());
        this.setNotificationType(getNotificationType());
        this.setJobNotificationLevels(getJobNotificationLevels());
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public String getDuns() {
        return duns;
    }

    public void setDuns(String duns) {
        this.duns = duns;
    }

    public String getSubscriptionType() {
        return subscriptionType;
    }

    public void setSubscriptionType(String subscriptionType) {
        this.subscriptionType = subscriptionType;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public Date getContractStartDate() {
        return contractStartDate;
    }

    public void setContractStartDate(Date contractStartDate) {
        this.contractStartDate = contractStartDate;
    }

    public Date getContractEndDate() {
        return contractEndDate;
    }

    public void setContractEndDate(Date contractEndDate) {
        this.contractEndDate = contractEndDate;
    }
}
