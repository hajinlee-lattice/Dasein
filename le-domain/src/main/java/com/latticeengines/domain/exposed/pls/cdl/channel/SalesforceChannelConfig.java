package com.latticeengines.domain.exposed.pls.cdl.channel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SalesforceChannelConfig implements ChannelConfig {

    @JsonProperty("accountLimit")
    private Long accountLimit;

    @JsonProperty("supressAccountWithoutAccountId")
    private Boolean supressAccountWithoutAccountId = false;

    public Long getAccoutLimit() {
        return accountLimit;
    }

    public void setAccountLimit(Long accountLimit) {
        this.accountLimit = accountLimit;
    }

    public Boolean isSupressAccountWithoutAccountId() {
        return supressAccountWithoutAccountId;
    }

    public void setSupressAccountWithoutAccountId(boolean supressAccountWithoutAccountId) {
        this.supressAccountWithoutAccountId = supressAccountWithoutAccountId;
    }

    @Override
    public ChannelConfig copyConfig(ChannelConfig config) {
        SalesforceChannelConfig salesforceChannelConfig = this;
        SalesforceChannelConfig newSalesforceChannelConfig = (SalesforceChannelConfig) config;
        salesforceChannelConfig.setAccountLimit(newSalesforceChannelConfig.getAccoutLimit());
        salesforceChannelConfig
                .setSupressAccountWithoutAccountId(newSalesforceChannelConfig.isSupressAccountWithoutAccountId());
        return this;

    }

}
