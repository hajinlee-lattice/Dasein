package com.latticeengines.domain.exposed.pls.cdl.channel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SalesforceChannelConfig extends ChannelConfig {

    @JsonProperty("showNumAccountsLaunched")
    private Boolean showNumAccountsLaunched = Boolean.FALSE;

    @JsonProperty("showNumAccountsSupressed")
    private Boolean showNumAccountsSupressed = Boolean.FALSE;

    @JsonProperty("showNumAccountsInSegment")
    private Boolean showNumAccountsInSegment = Boolean.FALSE;

    @JsonProperty("showNumContactsLaunched")
    private Boolean showNumContactsLaunched = Boolean.FALSE;

    @JsonProperty("showNumContactsSupressed")
    private Boolean showNumContactsSupressed = Boolean.FALSE;

    @JsonProperty("showNumContactsInSegment")
    private Boolean showNumContactsInSegment = Boolean.FALSE;

    @JsonProperty("supressAccountWithoutAccountId")
    private Boolean supressAccountWithoutAccountId = Boolean.FALSE;

    public Boolean isShowNumAccountsLaunched() {
        return showNumAccountsLaunched;
    }

    public void setShowNumAccountsLaunched(boolean showNumAccountsLaunched) {
        this.showNumAccountsLaunched = showNumAccountsLaunched;
    }

    public Boolean isShowNumAccountsSupressed() {
        return showNumAccountsSupressed;
    }

    public void setShowNumAccountsSupressed(boolean showNumAccountsSupressed) {
        this.showNumAccountsSupressed = showNumAccountsSupressed;
    }

    public Boolean isShowNumAccountsInSegment() {
        return showNumAccountsInSegment;
    }

    public void setShowNumAccountsInSegment(boolean showNumAccountsInSegment) {
        this.showNumAccountsInSegment = showNumAccountsInSegment;
    }

    public Boolean isShowNumContactsLaunched() {
        return showNumContactsLaunched;
    }

    public void setShowNumContactsLaunched(boolean showNumContactsLaunched) {
        this.showNumContactsLaunched = showNumContactsLaunched;
    }

    public Boolean isShowNumContactsSupressed() {
        return showNumContactsSupressed;
    }

    public void setShowNumContactsSupressed(boolean showNumContactsSupressed) {
        this.showNumContactsSupressed = showNumContactsSupressed;
    }

    public Boolean isShowNumContactsInSegment() {
        return showNumContactsInSegment;
    }

    public void setShowNumContactsInSegment(boolean showNumContactsInSegment) {
        this.showNumContactsInSegment = showNumContactsInSegment;
    }

    public Boolean isSupressAccountWithoutAccountId() {
        return supressAccountWithoutAccountId;
    }

    public void setSupressAccountWithoutAccountId(boolean supressAccountWithoutAccountId) {
        this.supressAccountWithoutAccountId = supressAccountWithoutAccountId;
    }

    @Override
    public ChannelConfig copyConfig(ChannelConfig config) {
        SalesforceChannelConfig salesforceChannelConfig = (SalesforceChannelConfig) super.copyConfig(config);
        SalesforceChannelConfig newSalesforceChannelConfig = (SalesforceChannelConfig) config;
        salesforceChannelConfig.setShowNumAccountsLaunched(newSalesforceChannelConfig.isShowNumAccountsLaunched());
        salesforceChannelConfig.setShowNumAccountsSupressed(newSalesforceChannelConfig.isShowNumAccountsSupressed());
        salesforceChannelConfig.setShowNumAccountsInSegment(newSalesforceChannelConfig.isShowNumAccountsInSegment());
        salesforceChannelConfig.setShowNumContactsLaunched(newSalesforceChannelConfig.isShowNumContactsLaunched());
        salesforceChannelConfig.setShowNumContactsSupressed(newSalesforceChannelConfig.isShowNumContactsSupressed());
        salesforceChannelConfig.setShowNumContactsInSegment(newSalesforceChannelConfig.isShowNumContactsInSegment());
        salesforceChannelConfig
                .setSupressAccountWithoutAccountId(newSalesforceChannelConfig.isSupressAccountWithoutAccountId());
        return this;

    }

}
