package com.latticeengines.domain.exposed.pls.cdl.channel;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class  FacebookChannelConfig implements ChannelConfig {

    private static final CDLExternalSystemName systemName = CDLExternalSystemName.Facebook;
    private static final AudienceType audienceType = AudienceType.CONTACTS;

    @JsonProperty("contactLimit")
    private Long contactLimit;

    @JsonProperty("suppressContactsWithoutEmails")
    private Boolean suppressContactsWithoutEmails = true;

    @JsonProperty("suppressAccountsWithoutContacts")
    private Boolean suppressAccountsWithoutContacts = true;

    @JsonProperty("audienceId")
    private String audienceId;

    @JsonProperty("audienceName")
    private String audienceName;

    public Long getContactLimit() {
        return contactLimit;
    }

    public void setContactLimit(Long contactLimit) {
        this.contactLimit = contactLimit;
    }

    @Override
    public boolean isSuppressContactsWithoutEmails() {
        return suppressContactsWithoutEmails;
    }

    public void setSuppressContactsWithoutEmails(boolean suppressContactsWithoutEmails) {
        this.suppressContactsWithoutEmails = suppressContactsWithoutEmails;
    }

    @Override
    public boolean isSuppressAccountsWithoutContacts() {
        return suppressAccountsWithoutContacts;
    }

    public void setSuppressAccountsWithoutContacts(boolean suppressAccountsWithoutContacts) {
        this.suppressAccountsWithoutContacts = true;
    }

    @Override
    @JsonProperty("suppressAccountsWithoutLookupId")
    public boolean isSuppressAccountsWithoutLookupId() {
        return false;
    }

    @Override
    public String getAudienceId() {
        return audienceId;
    }

    @Override
    public void setAudienceId(String audienceId) {
        this.audienceId = audienceId;
    }

    @Override
    public String getAudienceName() {
        return audienceName;
    }

    @Override
    public void setAudienceName(String audienceName) {
        this.audienceName = audienceName;
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
        if (!(channelConfig instanceof FacebookChannelConfig)) {
            return false;
        }
        FacebookChannelConfig updatedConfig = (FacebookChannelConfig) channelConfig;
        
        if (StringUtils.isBlank(updatedConfig.getAudienceId())) {
            return true;
        }

        return StringUtils.isBlank(this.audienceName) ? StringUtils.isNotBlank(updatedConfig.audienceName) //
                : !this.audienceName.equals(updatedConfig.audienceName);
    }

    @Override
    public void populateLaunchFromChannelConfig(PlayLaunch playLaunch) {
        playLaunch.setAudienceId(this.getAudienceId());
        playLaunch.setAudienceName(this.getAudienceName());
    }

    @Override
    public ChannelConfig copyConfig(ChannelConfig config) {
        FacebookChannelConfig facebookChannelConfig = this;
        FacebookChannelConfig newFacebookChannelConfig = (FacebookChannelConfig) config;
        facebookChannelConfig.setContactLimit(newFacebookChannelConfig.getContactLimit());
        facebookChannelConfig
                .setSuppressContactsWithoutEmails(newFacebookChannelConfig.isSuppressContactsWithoutEmails());
        facebookChannelConfig.setAudienceId(newFacebookChannelConfig.getAudienceId());
        facebookChannelConfig.setAudienceName(newFacebookChannelConfig.getAudienceName());
        return this;
    }

}
