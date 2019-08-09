package com.latticeengines.domain.exposed.pls.cdl.channel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SalesforceChannelConfig implements ChannelConfig {

    private static final CDLExternalSystemName systemName = CDLExternalSystemName.Salesforce;

    @JsonProperty("accountLimit")
    private Long accountLimit;

    @JsonProperty("supressAccountsWithoutLookupId")
    private Boolean supressAccountsWithoutLookupId = false;

    public Long getAccoutLimit() {
        return accountLimit;
    }

    public void setAccountLimit(Long accountLimit) {
        this.accountLimit = accountLimit;
    }

    public Boolean isSupressAccountsWithoutLookupId() {
        return supressAccountsWithoutLookupId;
    }

    public void setSupressAccountsWithoutLookupId(boolean supressAccountsWithoutLookupId) {
        this.supressAccountsWithoutLookupId = supressAccountsWithoutLookupId;
    }

    @Override
    public CDLExternalSystemName getSystemName() {
        return systemName;
    }

    @Override
    public ChannelConfig copyConfig(ChannelConfig config) {
        SalesforceChannelConfig salesforceChannelConfig = this;
        SalesforceChannelConfig newSalesforceChannelConfig = (SalesforceChannelConfig) config;
        salesforceChannelConfig.setAccountLimit(newSalesforceChannelConfig.getAccoutLimit());
        salesforceChannelConfig
                .setSupressAccountsWithoutLookupId(newSalesforceChannelConfig.isSupressAccountsWithoutLookupId());
        return this;

    }

}
