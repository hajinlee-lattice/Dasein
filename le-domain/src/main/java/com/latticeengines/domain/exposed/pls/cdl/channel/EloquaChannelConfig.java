package com.latticeengines.domain.exposed.pls.cdl.channel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EloquaChannelConfig implements ChannelConfig {

    @JsonProperty("contactLimit")
    private Long contactLimit;

    @JsonProperty("supressContactsWithoutEmails")
    private Boolean supressContactsWithoutEmails = false;

    @JsonProperty("supressAccountWithoutContacts")
    private Boolean supressAccountWithoutContacts = false;

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

    @Override
    public ChannelConfig copyConfig(ChannelConfig config) {
        EloquaChannelConfig eloquaChannelConfig = this;
        EloquaChannelConfig newEloquaChannelConfig = (EloquaChannelConfig) config;
        eloquaChannelConfig.setContactLimit(newEloquaChannelConfig.getContactLimit());
        eloquaChannelConfig.setSupressContactsWithoutEmails(newEloquaChannelConfig.isSupressContactsWithoutEmails());
        eloquaChannelConfig.setSupressAccountWithoutContacts(newEloquaChannelConfig.isSupressAccountWithoutContacts());
        return this;

    }

}
