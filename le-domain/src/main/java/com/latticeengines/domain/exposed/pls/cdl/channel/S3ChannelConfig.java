package com.latticeengines.domain.exposed.pls.cdl.channel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class S3ChannelConfig implements ChannelConfig {

    @JsonProperty("audienceType")
    private AudienceType audienceType;

    @JsonProperty("accountLimit")
    private Long accountLimit;

    // @JsonProperty("showNumAccountsLaunched")
    // private Boolean showNumAccountsLaunched = Boolean.FALSE;
    //
    // @JsonProperty("showNumAccountsSupressed")
    // private Boolean showNumAccountsSupressed = Boolean.FALSE;
    //
    // @JsonProperty("showNumAccountsInSegment")
    // private Boolean showNumAccountsInSegment = Boolean.FALSE;
    //
    // @JsonProperty("showNumContactsLaunched")
    // private Boolean showNumContactsLaunched = Boolean.FALSE;
    //
    // @JsonProperty("showNumContactsSupressed")
    // private Boolean showNumContactsSupressed = Boolean.FALSE;
    //
    // @JsonProperty("showNumContactsInSegment")
    // private Boolean showNumContactsInSegment = Boolean.FALSE;

    @JsonProperty("supressAccountWithoutAccountId")
    private Boolean supressAccountWithoutAccountId = Boolean.FALSE;

    @JsonProperty("supressAccountWithoutContacts")
    private Boolean supressAccountWithoutContacts = Boolean.FALSE;

    // public Boolean isCreateAccountBasedAudience() {
    // return createAccountBasedAudience;
    // }
    //
    // public void setCreateAccountBasedAudience(boolean
    // createAccountBasedAudience) {
    // this.createAccountBasedAudience = createAccountBasedAudience;
    // }
    //
    // public Boolean isCreateContactBasedAudience() {
    // return createContactBasedAudience;
    // }
    //
    // public void setCreateContactBasedAudience(boolean
    // createContactBasedAudience) {
    // this.createContactBasedAudience = createContactBasedAudience;
    // }

    // public Boolean isShowNumAccountsLaunched() {
    // return showNumAccountsLaunched;
    // }
    //
    // public void setShowNumAccountsLaunched(boolean showNumAccountsLaunched) {
    // this.showNumAccountsLaunched = showNumAccountsLaunched;
    // }
    //
    // public Boolean isShowNumAccountsSupressed() {
    // return showNumAccountsSupressed;
    // }
    //
    // public void setShowNumAccountsSupressed(boolean showNumAccountsSupressed)
    // {
    // this.showNumAccountsSupressed = showNumAccountsSupressed;
    // }
    //
    // public Boolean isShowNumAccountsInSegment() {
    // return showNumAccountsInSegment;
    // }
    //
    // public void setShowNumAccountsInSegment(boolean showNumAccountsInSegment)
    // {
    // this.showNumAccountsInSegment = showNumAccountsInSegment;
    // }
    //
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

    public Long getAccoutLimit() {
        return accountLimit;
    }

    public void setAccountLimit(Long accountLimit) {
        this.accountLimit = accountLimit;
    }

    public AudienceType getAudienceType() {
        return audienceType;
    }

    public void setAudienceType(AudienceType audienceType) {
        this.audienceType = audienceType;
    }

    public Boolean isSupressAccountWithoutAccountId() {
        return supressAccountWithoutAccountId;
    }

    public void setSupressAccountWithoutAccountId(boolean supressAccountWithoutAccountId) {
        this.supressAccountWithoutAccountId = supressAccountWithoutAccountId;
    }

    public Boolean isSupressAccountWithoutContacts() {
        return supressAccountWithoutContacts;
    }

    public void setSupressAccountWithoutContacts(boolean supressAccountWithoutContacts) {
        this.supressAccountWithoutContacts = supressAccountWithoutContacts;
    }

    @Override
    public ChannelConfig copyConfig(ChannelConfig config) {
        S3ChannelConfig s3ChannelConfig = this;
        S3ChannelConfig newS3ChannelConfig = (S3ChannelConfig) config;
        s3ChannelConfig.setAudienceType(newS3ChannelConfig.getAudienceType());
        // s3ChannelConfig.setShowNumAccountsLaunched(newS3ChannelConfig.isShowNumAccountsLaunched());
        // s3ChannelConfig.setShowNumAccountsSupressed(newS3ChannelConfig.isShowNumAccountsSupressed());
        // s3ChannelConfig.setShowNumAccountsInSegment(newS3ChannelConfig.isShowNumAccountsInSegment());
        // s3ChannelConfig.setShowNumContactsLaunched(newS3ChannelConfig.isShowNumContactsLaunched());
        // s3ChannelConfig.setShowNumContactsSupressed(newS3ChannelConfig.isShowNumContactsSupressed());
        // s3ChannelConfig.setShowNumContactsInSegment(newS3ChannelConfig.isShowNumContactsInSegment());
        s3ChannelConfig.setAccountLimit(newS3ChannelConfig.getAccoutLimit());
        s3ChannelConfig.setSupressAccountWithoutAccountId(newS3ChannelConfig.isSupressAccountWithoutAccountId());
        s3ChannelConfig.setSupressAccountWithoutContacts(newS3ChannelConfig.isSupressAccountWithoutContacts());
        return this;

    }

}
