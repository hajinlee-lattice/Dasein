package com.latticeengines.domain.exposed.pls.cdl.channel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MarketoChannelConfig implements ChannelConfig {

    private static final CDLExternalSystemName systemName = CDLExternalSystemName.Marketo;
    private static final AudienceType audienceType = AudienceType.CONTACTS;

    @JsonProperty("contactLimit")
    private Long contactLimit;

    @JsonProperty("suppressContactsWithoutEmails")
    private Boolean suppressContactsWithoutEmails = false;

    @JsonProperty("suppressAccountWithoutContacts")
    private Boolean suppressAccountWithoutContacts = false;

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
        return suppressContactsWithoutEmails;
    }

    public void setSuppressContactsWithoutEmails(boolean suppressContactsWithoutEmails) {
        this.suppressContactsWithoutEmails = suppressContactsWithoutEmails;
    }

    public Boolean isSupressAccountWithoutContacts() {
        return suppressAccountWithoutContacts;
    }

    public void setSuppressAccountWithoutContacts(boolean suppressAccountWithoutContacts) {
        this.suppressAccountWithoutContacts = suppressAccountWithoutContacts;
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
        MarketoChannelConfig marketoChannelConfig = this;
        MarketoChannelConfig newMarketoChannelConfig = (MarketoChannelConfig) config;
        marketoChannelConfig.setContactLimit(newMarketoChannelConfig.getContactLimit());
        marketoChannelConfig.setSuppressContactsWithoutEmails(newMarketoChannelConfig.isSupressContactsWithoutEmails());
        marketoChannelConfig
                .setSuppressAccountWithoutContacts(newMarketoChannelConfig.isSupressAccountWithoutContacts());
        marketoChannelConfig.setAudienceId(newMarketoChannelConfig.getAudienceId());
        marketoChannelConfig.setAudienceName(newMarketoChannelConfig.getAudienceName());
        marketoChannelConfig.setFolderName(newMarketoChannelConfig.getFolderName());
        return this;

    }
}
