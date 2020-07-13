package com.latticeengines.domain.exposed.pls.cdl.channel;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OutreachChannelConfig implements ChannelConfig {

    private static final CDLExternalSystemName systemName = CDLExternalSystemName.Outreach;
    private static final AudienceType audienceType = AudienceType.CONTACTS;

    @JsonProperty("contactLimit")
    private Long contactLimit;

    @JsonProperty("suppressContactsWithoutEmails")
    private boolean suppressContactsWithoutEmails = true;

    @JsonProperty("suppressAccountsWithoutContacts")
    private boolean suppressAccountsWithoutContacts = true;

    @JsonProperty("suppressAccountsWithoutLookupId")
    private Boolean suppressAccountsWithoutLookupId = true;

    @JsonProperty("audienceId")
    private String audienceId;

    @JsonProperty("audienceName")
    private String audienceName;

    @JsonProperty("folderId")
    private String folderId;

    @JsonProperty("folderName")
    private String folderName;

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
    public boolean isSuppressAccountsWithoutLookupId() {
        return suppressAccountsWithoutLookupId;
    }

    public void setSuppressAccountsWithoutLookupId(boolean suppressAccountsWithoutLookupId) {
        this.suppressAccountsWithoutLookupId = suppressAccountsWithoutLookupId;
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

    @Override
    public boolean shouldResetDeltaCalculations(ChannelConfig channelConfig) {
        if (!(channelConfig instanceof OutreachChannelConfig)) {
            return false;
        }
        OutreachChannelConfig updatedConfig = (OutreachChannelConfig) channelConfig;

        return StringUtils.isBlank(this.audienceName) ? StringUtils.isNotBlank(updatedConfig.audienceName) //
                : !this.audienceName.equals(updatedConfig.audienceName);
    }

    @Override
    public void populateLaunchFromChannelConfig(PlayLaunch playLaunch) {
        playLaunch.setAudienceId(this.getAudienceId());
        playLaunch.setAudienceName(this.getAudienceName());
        playLaunch.setFolderId(this.getFolderId());
        playLaunch.setFolderName(this.getFolderName());
    }

    @Override
    public ChannelConfig copyConfig(ChannelConfig config) {
        OutreachChannelConfig outreachChannelConfig = this;
        OutreachChannelConfig newOutreachChannelConfig = (OutreachChannelConfig) config;
        outreachChannelConfig.setContactLimit(newOutreachChannelConfig.getContactLimit());
        outreachChannelConfig
                .setSuppressContactsWithoutEmails(newOutreachChannelConfig.isSuppressContactsWithoutEmails());
        outreachChannelConfig
                .setSuppressAccountsWithoutContacts(newOutreachChannelConfig.isSuppressAccountsWithoutContacts());
        outreachChannelConfig
                .setSuppressAccountsWithoutLookupId(newOutreachChannelConfig.isSuppressAccountsWithoutLookupId());
        outreachChannelConfig.setAudienceId(newOutreachChannelConfig.getAudienceId());
        outreachChannelConfig.setAudienceName(newOutreachChannelConfig.getAudienceName());
        outreachChannelConfig.setFolderId(newOutreachChannelConfig.getFolderId());
        outreachChannelConfig.setFolderName(newOutreachChannelConfig.getFolderName());
        return this;

    }
}
