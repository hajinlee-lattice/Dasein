package com.latticeengines.domain.exposed.pls.cdl.channel;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class LinkedInChannelConfig implements ChannelConfig {

    private static final CDLExternalSystemName systemName = CDLExternalSystemName.LinkedIn;

    @JsonProperty("contactLimit")
    private Long contactLimit;

    @JsonProperty("suppressContactsWithoutEmails")
    private boolean suppressContactsWithoutEmails = true;

    @JsonProperty("suppressAccountsWithoutNameOrDomain")
    private boolean suppressAccountsWithoutNameOrDomain = false;

    @JsonProperty("audienceId")
    private String audienceId;

    @JsonProperty("audienceName")
    private String audienceName;

    @JsonProperty("folderName")
    private String folderName;

    @JsonProperty("audienceType")
    private AudienceType audienceType;

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

    @JsonProperty("suppressAccountsWithoutContacts")
    public boolean isSuppressAccountsWithoutContacts() { return audienceType != AudienceType.ACCOUNTS; }

    public boolean isSuppressAccountsWithoutNameOrDomain() {
        return suppressAccountsWithoutNameOrDomain;
    }

    public void setSuppressAccountsWithoutNameOrDomain(boolean suppressAccountsWithoutNameOrDomain) {
        this.suppressAccountsWithoutNameOrDomain = suppressAccountsWithoutNameOrDomain;
    }

    @Override
    @JsonProperty("suppressAccountsWithoutLookupId")
    public boolean isSuppressAccountsWithoutLookupId() {
        return false;
    }

    public String getAudienceId() {
        return audienceId;
    }

    public void setAudienceId(String audienceId) {
        this.audienceId = audienceId;
    }

    public String getAudienceName() {
        return audienceName;
    }

    public void setAudienceName(String audienceName) {
        this.audienceName = audienceName;
    }

    @Override
    public AudienceType getAudienceType() {
        return audienceType;
    }

    public void setAudienceType(AudienceType audienceType) {
        this.audienceType = audienceType;
    }

    public String getFolderName() {
        return folderName;
    }

    public void setFolderName(String folderName) {
        this.folderName = folderName;
    }

    @Override
    public CDLExternalSystemName getSystemName() {
        return systemName;
    }

    @Override
    public boolean shouldResetDeltaCalculations(ChannelConfig channelConfig) {
        if (!(channelConfig instanceof LinkedInChannelConfig)) {
            return false;
        }
        LinkedInChannelConfig updatedConfig = (LinkedInChannelConfig) channelConfig;

        return (this.audienceType == null ? updatedConfig.audienceType != null //
                : !this.audienceType.equals(updatedConfig.audienceType)) //
                || (StringUtils.isBlank(this.audienceName) ? StringUtils.isNotBlank(updatedConfig.audienceName) //
                        : !this.audienceName.equals(updatedConfig.audienceName));
    }

    @Override
    public void populateLaunchFromChannelConfig(PlayLaunch playLaunch) {
        playLaunch.setAudienceId(this.getAudienceId());
        playLaunch.setAudienceName(this.getAudienceName());
    }

    @Override
    public ChannelConfig copyConfig(ChannelConfig config) {
        LinkedInChannelConfig linkedinChannelConfig = this;
        LinkedInChannelConfig newLinkedInChannelConfig = (LinkedInChannelConfig) config;
        linkedinChannelConfig.setContactLimit(newLinkedInChannelConfig.getContactLimit());
        linkedinChannelConfig
                .setSuppressContactsWithoutEmails(newLinkedInChannelConfig.isSuppressContactsWithoutEmails());
        linkedinChannelConfig.setAudienceId(newLinkedInChannelConfig.getAudienceId());
        linkedinChannelConfig.setAudienceName(newLinkedInChannelConfig.getAudienceName());
        linkedinChannelConfig.setFolderName(newLinkedInChannelConfig.getFolderName());
        linkedinChannelConfig.setAudienceType(newLinkedInChannelConfig.getAudienceType());
        return this;
    }

}
