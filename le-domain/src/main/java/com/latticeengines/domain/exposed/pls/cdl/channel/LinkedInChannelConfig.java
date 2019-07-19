package com.latticeengines.domain.exposed.pls.cdl.channel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;

import javax.persistence.Transient;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class LinkedInChannelConfig implements ChannelConfig {

    private static final CDLExternalSystemName systemName = CDLExternalSystemName.LinkedIn;

    @JsonProperty("contactLimit")
    private Long contactLimit;

    @JsonProperty("supressContactsWithoutEmails")
    private Boolean supressContactsWithoutEmails = false;

    @JsonProperty("supressAccountWithoutContacts")
    private Boolean supressAccountWithoutContacts = false;

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

    public Boolean isSupressContactsWithoutEmails() {
        return supressContactsWithoutEmails;
    }

    public void setSupressContactsWithoutEmails(boolean supressContactsWithoutEmails) {
        this.supressContactsWithoutEmails = supressContactsWithoutEmails;
    }

    public Boolean isSupressAccountWithoutContacts() {
        return supressAccountWithoutContacts;
    }

    public void setSupressAccountWithoutContacts(boolean supressAccountWithoutContacts) {
        this.supressAccountWithoutContacts = supressAccountWithoutContacts;
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
    public ChannelConfig copyConfig(ChannelConfig config) {
        LinkedInChannelConfig linkedinChannelConfig = this;
        LinkedInChannelConfig newLinkedInChannelConfig = (LinkedInChannelConfig) config;
        linkedinChannelConfig.setContactLimit(newLinkedInChannelConfig.getContactLimit());
        linkedinChannelConfig.setSupressContactsWithoutEmails(newLinkedInChannelConfig.isSupressContactsWithoutEmails());
        linkedinChannelConfig
                .setSupressAccountWithoutContacts(newLinkedInChannelConfig.isSupressAccountWithoutContacts());
        linkedinChannelConfig.setAudienceId(newLinkedInChannelConfig.getAudienceId());
        linkedinChannelConfig.setAudienceName(newLinkedInChannelConfig.getAudienceName());
        linkedinChannelConfig.setFolderName(newLinkedInChannelConfig.getFolderName());
        linkedinChannelConfig.setAudienceType(newLinkedInChannelConfig.getAudienceType());
        return this;

    }
}
