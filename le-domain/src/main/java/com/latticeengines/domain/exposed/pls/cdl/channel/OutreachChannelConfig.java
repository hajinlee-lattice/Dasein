package com.latticeengines.domain.exposed.pls.cdl.channel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OutreachChannelConfig implements ChannelConfig {

    private static final CDLExternalSystemName systemName = CDLExternalSystemName.Outreach;
    private static final AudienceType audienceType = AudienceType.CONTACTS;

    @JsonProperty("contactLimit")
    private Long contactLimit;

    @JsonProperty("supressContactsWithoutEmails")
    private Boolean supressContactsWithoutEmails = true;

    @JsonProperty("supressAccountWithoutContacts")
    private Boolean supressAccountWithoutContacts = true;

    @JsonProperty("audienceId")
    private String audienceId;

    @JsonProperty("audienceName")
    private String audienceName;

    @JsonProperty("folderName")
    private String folderName;

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
    public ChannelConfig copyConfig(ChannelConfig config) {
        OutreachChannelConfig outreachChannelConfig = this;
        OutreachChannelConfig newOutreachChannelConfig = (OutreachChannelConfig) config;
        outreachChannelConfig.setContactLimit(newOutreachChannelConfig.getContactLimit());
        outreachChannelConfig.setSupressContactsWithoutEmails(newOutreachChannelConfig.isSupressContactsWithoutEmails());
        outreachChannelConfig
                .setSupressAccountWithoutContacts(newOutreachChannelConfig.isSupressAccountWithoutContacts());
        outreachChannelConfig.setAudienceId(newOutreachChannelConfig.getAudienceId());
        outreachChannelConfig.setAudienceName(newOutreachChannelConfig.getAudienceName());
        outreachChannelConfig.setFolderName(newOutreachChannelConfig.getFolderName());
        return this;

    }
}
