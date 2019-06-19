package com.latticeengines.domain.exposed.pls.cdl.channel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EloquaChannelConfig implements ChannelConfig {

    @JsonProperty("accountLimit")
    private Long accountLimit;

    // @JsonProperty("showNumContactsLaunched")
    // private Boolean showNumContactsLaunched = Boolean.FALSE;
    //
    // @JsonProperty("showNumContactsSupressed")
    // private Boolean showNumContactsSupressed = Boolean.FALSE;
    //
    // @JsonProperty("showNumContactsInSegment")
    // private Boolean showNumContactsInSegment = Boolean.FALSE;
    //
    // @JsonProperty("showNumContactsWithDupeEmails")
    // private Boolean showNumContactsWithDupeEmails = Boolean.FALSE;

    @JsonProperty("supressContactsWithoutEmails")
    private Boolean supressContactsWithoutEmails = Boolean.FALSE;

    @JsonProperty("supressAccountWithoutContacts")
    private Boolean supressAccountWithoutContacts = Boolean.FALSE;

    // public Boolean isShowNumContactsLaunched() {
    // return showNumContactsLaunched;
    // }
    //
    // public void setShowNumContactsLaunched(boolean showNumContactsLaunched) {
    // this.showNumContactsLaunched = showNumContactsLaunched;
    // }
    //
    // public Boolean isShowNumContactsSupressed() {
    // return showNumContactsSupressed;
    // }
    //
    // public void setShowNumContactsSupressed(boolean showNumContactsSupressed)
    // {
    // this.showNumContactsSupressed = showNumContactsSupressed;
    // }
    //
    // public Boolean isShowNumContactsInSegment() {
    // return showNumContactsInSegment;
    // }
    //
    // public void setShowNumContactsInSegment(boolean showNumContactsInSegment)
    // {
    // this.showNumContactsInSegment = showNumContactsInSegment;
    // }
    //
    // public Boolean isShowNumContactsWithDupeEmails() {
    // return showNumContactsWithDupeEmails;
    // }
    //
    // public void setShowNumContactsWithDupeEmails(boolean
    // showNumContactsWithDupeEmails) {
    // this.showNumContactsWithDupeEmails = showNumContactsWithDupeEmails;
    // }

    public Long getAccoutLimit() {
        return accountLimit;
    }

    public void setAccountLimit(Long accountLimit) {
        this.accountLimit = accountLimit;
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
        // eloquaChannelConfig.setShowNumContactsLaunched(newEloquaChannelConfig.isShowNumContactsLaunched());
        // eloquaChannelConfig.setShowNumContactsSupressed(newEloquaChannelConfig.isShowNumContactsSupressed());
        // eloquaChannelConfig.setShowNumContactsInSegment(newEloquaChannelConfig.isShowNumContactsInSegment());
        // eloquaChannelConfig.setShowNumContactsWithDupeEmails(newEloquaChannelConfig.isShowNumContactsWithDupeEmails());
        eloquaChannelConfig.setAccountLimit(newEloquaChannelConfig.getAccoutLimit());
        eloquaChannelConfig.setSupressContactsWithoutEmails(newEloquaChannelConfig.isSupressContactsWithoutEmails());
        eloquaChannelConfig.setSupressAccountWithoutContacts(newEloquaChannelConfig.isSupressAccountWithoutContacts());
        return this;

    }

}
