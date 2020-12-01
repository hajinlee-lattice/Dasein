package com.latticeengines.domain.exposed.pls.cdl.channel;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.LaunchBaseType;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OutreachChannelConfig implements ChannelConfig {

    private static final CDLExternalSystemName systemName = CDLExternalSystemName.Outreach;

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

    @JsonProperty("audienceType")
    private AudienceType audienceType;

    @JsonProperty("launchBaseType")
    private LaunchBaseType launchBaseType;

    @JsonProperty("taskDescription")
    private String taskDescription;

    @JsonProperty("taskPriority")
    private String taskPriority;

    @JsonProperty("taskType")
    private String taskType;

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

    @Override
    @JsonProperty("audienceType")
    public AudienceType getAudienceType() {
        if (audienceType == null) {
            audienceType = AudienceType.CONTACTS;
        }
        return audienceType;
    }

    public void setAudienceType(AudienceType audienceType) {
        this.audienceType = audienceType;
    }

    @Override
    public CDLExternalSystemName getSystemName() {
        return systemName;
    }

    public LaunchBaseType getLaunchBaseType() {
        return launchBaseType;
    }

    public void setLaunchBaseType(LaunchBaseType launchBaseType) {
        this.launchBaseType = launchBaseType;
    }

    public String getTaskDescription() {
        return taskDescription;
    }

    public void setTaskDescription(String taskDescription) {
        this.taskDescription = taskDescription;
    }

    public String getTaskPriority() {
        return taskPriority;
    }

    public void setTaskPriority(String taskPriority) {
        this.taskPriority = taskPriority;
    }

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    @Override
    public boolean shouldResetDeltaCalculations(ChannelConfig channelConfig) {
        if (!(channelConfig instanceof OutreachChannelConfig)) {
            return false;
        }
        OutreachChannelConfig updatedConfig = (OutreachChannelConfig) channelConfig;

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
        outreachChannelConfig.setAudienceType(newOutreachChannelConfig.getAudienceType());
        outreachChannelConfig.setLaunchBaseType(newOutreachChannelConfig.getLaunchBaseType());
        outreachChannelConfig.setTaskDescription(newOutreachChannelConfig.getTaskDescription());
        outreachChannelConfig.setTaskPriority(newOutreachChannelConfig.getTaskPriority());
        outreachChannelConfig.setTaskType(newOutreachChannelConfig.getTaskType());
        return this;

    }
}
