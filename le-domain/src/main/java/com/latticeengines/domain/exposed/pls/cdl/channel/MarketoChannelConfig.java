package com.latticeengines.domain.exposed.pls.cdl.channel;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MarketoChannelConfig implements ChannelConfig {

    private static final CDLExternalSystemName systemName = CDLExternalSystemName.Marketo;
    private static final AudienceType audienceType = AudienceType.CONTACTS;

    @JsonProperty("contactLimit")
    private Long contactLimit;

    @JsonProperty("suppressContactsWithoutEmails")
    private boolean suppressContactsWithoutEmails = true;

    @JsonProperty("suppressAccountsWithoutContacts")
    private boolean suppressAccountsWithoutContacts = true;

    @JsonProperty("audienceId")
    private String audienceId;

    @JsonProperty("audienceName")
    private String audienceName;

    @JsonProperty("folderId")
    private String folderId;

    @JsonProperty("folderName")
    private String folderName;

    @JsonProperty("earliestUpdatedDays")
    private String earliestUpdatedDays;

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
        this.suppressAccountsWithoutContacts = suppressAccountsWithoutContacts;
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

    public String getFolderId() {
        return folderId;
    }

    public void setFolderId(String folderId) {
        this.folderId = folderId;
    }

    public String getFolderName() {
        return folderName;
    }

    public void setFolderName(String folderName) {
        this.folderName = folderName;
    }

    @Override
    public AudienceType getAudienceType() {
        return audienceType;
    }

    @Override
    public CDLExternalSystemName getSystemName() {
        return systemName;
    }

    public void setEarliestUpdatedDays(String earliestUpdatedDays) {
        this.earliestUpdatedDays = earliestUpdatedDays;
    }

    public String getEarliestUpdatedDays() {
        return earliestUpdatedDays;
    }

    @Override
    public void populateLaunchFromChannelConfig(PlayLaunch playLaunch) {
        playLaunch.setAudienceId(this.getAudienceId());
        playLaunch.setAudienceName(this.getAudienceName());
        playLaunch.setFolderId(this.getFolderId());
        playLaunch.setFolderName(this.getFolderName());
    }

    @Override
    public boolean shouldResetDeltaCalculations(ChannelConfig channelConfig) {
        if (!(channelConfig instanceof MarketoChannelConfig)) {
            return false;
        }
        MarketoChannelConfig updatedConfig = (MarketoChannelConfig) channelConfig;

        if (StringUtils.isBlank(updatedConfig.getAudienceId())) {
            return true;
        }

        return (StringUtils.isBlank(this.audienceId) ? StringUtils.isNotBlank(updatedConfig.audienceId) //
                : !this.audienceId.equals(updatedConfig.audienceId)) //
                || (StringUtils.isBlank(this.audienceName) ? StringUtils.isNotBlank(updatedConfig.audienceName) //
                        : !this.audienceName.equals(updatedConfig.audienceName)) //
                || (StringUtils.isBlank(this.folderName) ? StringUtils.isNotBlank(updatedConfig.folderName) //
                        : !this.folderName.equals(updatedConfig.folderName));
    }

    @Override
    public ChannelConfig copyConfig(ChannelConfig config) {
        MarketoChannelConfig marketoChannelConfig = this;
        MarketoChannelConfig newMarketoChannelConfig = (MarketoChannelConfig) config;
        marketoChannelConfig.setContactLimit(newMarketoChannelConfig.getContactLimit());
        marketoChannelConfig
                .setSuppressContactsWithoutEmails(newMarketoChannelConfig.isSuppressContactsWithoutEmails());
        marketoChannelConfig
                .setSuppressAccountsWithoutContacts(newMarketoChannelConfig.isSuppressAccountsWithoutContacts());
        marketoChannelConfig.setAudienceId(newMarketoChannelConfig.getAudienceId());
        marketoChannelConfig.setAudienceName(newMarketoChannelConfig.getAudienceName());
        marketoChannelConfig.setFolderId(newMarketoChannelConfig.getFolderId());
        marketoChannelConfig.setFolderName(newMarketoChannelConfig.getFolderName());
        marketoChannelConfig.setEarliestUpdatedDays(newMarketoChannelConfig.getEarliestUpdatedDays());
        return this;
    }
}
