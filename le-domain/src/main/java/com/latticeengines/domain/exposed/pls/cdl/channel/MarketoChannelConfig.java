package com.latticeengines.domain.exposed.pls.cdl.channel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MarketoChannelConfig implements ChannelConfig {

    @JsonProperty("contactLimit")
    private Long contactLimit;

    @JsonProperty("supressContactsWithoutEmails")
    private Boolean supressContactsWithoutEmails = Boolean.FALSE;

    @JsonProperty("supressAccountWithoutContacts")
    private Boolean supressAccountWithoutContacts = Boolean.FALSE;

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
    public ChannelConfig copyConfig(ChannelConfig config) {
        MarketoChannelConfig marketoChannelConfig = this;
        MarketoChannelConfig newMarketoChannelConfig = (MarketoChannelConfig) config;
        marketoChannelConfig.setContactLimit(newMarketoChannelConfig.getContactLimit());
        marketoChannelConfig.setSupressContactsWithoutEmails(newMarketoChannelConfig.isSupressContactsWithoutEmails());
        marketoChannelConfig
                .setSupressAccountWithoutContacts(newMarketoChannelConfig.isSupressAccountWithoutContacts());
        marketoChannelConfig.setAudienceId(newMarketoChannelConfig.getAudienceId());
        marketoChannelConfig.setAudienceName(newMarketoChannelConfig.getAudienceName());
        marketoChannelConfig.setFolderName(newMarketoChannelConfig.getFolderName());
        return this;

    }

}
