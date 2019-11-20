package com.latticeengines.domain.exposed.pls.cdl.channel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EloquaChannelConfig implements ChannelConfig {

    private static final CDLExternalSystemName systemName = CDLExternalSystemName.Eloqua;
    private static final AudienceType audienceType = AudienceType.CONTACTS;

    @JsonProperty("contactLimit")
    private Long contactLimit;

    @JsonProperty("suppressContactsWithoutEmails")
    private Boolean suppressContactsWithoutEmails = true;

    @JsonProperty("suppressAccountsWithoutContacts")
    private Boolean suppressAccountsWithoutContacts = true;

    public Long getContactLimit() {
        return contactLimit;
    }

    public void setContactLimit(Long contactLimit) {
        this.contactLimit = contactLimit;
    }

    public boolean isSuppressContactsWithoutEmails() {
        return suppressContactsWithoutEmails;
    }

    public void setSuppressContactsWithoutEmails(boolean suppressContactsWithoutEmails) {
        this.suppressContactsWithoutEmails = suppressContactsWithoutEmails;
    }

    public boolean isSuppressAccountsWithoutContacts() {
        return suppressAccountsWithoutContacts;
    }

    public void setSuppressAccountsWithoutContacts(boolean suppressAccountsWithoutContacts) {
        this.suppressAccountsWithoutContacts = suppressAccountsWithoutContacts;
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
    public boolean shouldResetDeltaCalculations(ChannelConfig channelConfig) {
        return false;
    }

    @Override
    public void populateLaunchFromChannelConfig(PlayLaunch playLaunch) {
        // No special Launch properties to set in Play launch for eloqua
    }

    @Override
    public void setAudienceName(String audienceName) {

    }

    @Override
    public String getAudienceName() {
        return "";
    }

    @Override
    public void setAudienceId(String audienceId) {

    }

    @Override
    public String getAudienceId() {
        return "";
    }

    @Override
    public ChannelConfig copyConfig(ChannelConfig config) {
        EloquaChannelConfig eloquaChannelConfig = this;
        EloquaChannelConfig newEloquaChannelConfig = (EloquaChannelConfig) config;
        eloquaChannelConfig.setContactLimit(newEloquaChannelConfig.getContactLimit());
        eloquaChannelConfig.setSuppressContactsWithoutEmails(newEloquaChannelConfig.isSuppressContactsWithoutEmails());
        eloquaChannelConfig
                .setSuppressAccountsWithoutContacts(newEloquaChannelConfig.isSuppressAccountsWithoutContacts());
        return this;
    }
}
