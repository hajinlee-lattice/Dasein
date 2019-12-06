package com.latticeengines.domain.exposed.pls.cdl.channel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SalesforceChannelConfig implements ChannelConfig {

    private static final CDLExternalSystemName systemName = CDLExternalSystemName.Salesforce;
    private static final AudienceType audienceType = AudienceType.ACCOUNTS;

    @JsonProperty("accountLimit")
    private Long accountLimit;

    @JsonProperty("suppressAccountsWithoutLookupId")
    private boolean suppressAccountsWithoutLookupId = false;

    @Override
    @JsonProperty("suppressContactsWithoutEmails")
    public boolean isSuppressContactsWithoutEmails() { return false; }

    @Override
    @JsonProperty("suppressAccountsWithoutContacts")
    public boolean isSuppressAccountsWithoutContacts() { return false; }

    public Long getAccountLimit() {
        return accountLimit;
    }

    public void setAccountLimit(Long accountLimit) {
        this.accountLimit = accountLimit;
    }

    @Override
    public boolean isSuppressAccountsWithoutLookupId() {
        return suppressAccountsWithoutLookupId;
    }

    public void setSuppressAccountsWithoutLookupId(boolean suppressAccountsWithoutLookupId) {
        this.suppressAccountsWithoutLookupId = suppressAccountsWithoutLookupId;
    }

    @Override
    public AudienceType getAudienceType() {
        return audienceType;
    }

    @Override
    public CDLExternalSystemName getSystemName() {
        return systemName;
    }

    @Override
    public void setAudienceId(String audienceId) {

    }

    @Override
    public String getAudienceId() {
        return "";
    }

    @Override
    public void setAudienceName(String audienceName) {

    }

    @Override
    public String getAudienceName() {
        return "";
    }

    @Override
    public boolean shouldResetDeltaCalculations(ChannelConfig channelConfig) {
        return false;
    }

    @Override
    public void populateLaunchFromChannelConfig(PlayLaunch playLaunch) {
        playLaunch.setExcludeItemsWithoutSalesforceId(this.isSuppressAccountsWithoutLookupId());
    }

    @Override
    public ChannelConfig copyConfig(ChannelConfig config) {
        SalesforceChannelConfig salesforceChannelConfig = this;
        SalesforceChannelConfig newSalesforceChannelConfig = (SalesforceChannelConfig) config;
        salesforceChannelConfig.setAccountLimit(newSalesforceChannelConfig.getAccountLimit());
        salesforceChannelConfig
                .setSuppressAccountsWithoutLookupId(newSalesforceChannelConfig.isSuppressAccountsWithoutLookupId());
        return this;

    }

}
