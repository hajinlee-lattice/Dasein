package com.latticeengines.domain.exposed.pls.cdl.channel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class S3ChannelConfig extends ChannelConfig {

    @JsonProperty("createAccountBasedAudience")
    private Boolean createAccountBasedAudience = Boolean.FALSE;

    @JsonProperty("createContactBasedAudience")
    private Boolean createContactBasedAudience = Boolean.FALSE;

    @JsonProperty("showNumAccountsLaunched")
    private Boolean showNumAccountsLaunched = Boolean.FALSE;

    @JsonProperty("showNumAccountsSupressed")
    private Boolean showNumAccountsSupressed = Boolean.FALSE;

    @JsonProperty("showNumAccountsInSegment")
    private Boolean showNumAccountsInSegment = Boolean.FALSE;

    @JsonProperty("showNumContactsLaunched")
    private Boolean showNumContactsLaunched = Boolean.FALSE;

    @JsonProperty("showNumContactsSupressed")
    private Boolean showNumContactsSupressed = Boolean.FALSE;

    @JsonProperty("showNumContactsInSegment")
    private Boolean showNumContactsInSegment = Boolean.FALSE;

    @JsonProperty("supressAccountWithoutAccountId")
    private Boolean supressAccountWithoutAccountId = Boolean.FALSE;

    @JsonProperty("supressAccountWithoutContacts")
    private Boolean supressAccountWithoutContacts = Boolean.FALSE;

    public Boolean isCreateAccountBasedAudience() {
        return createAccountBasedAudience;
    }

    public void setCreateAccountBasedAudience(boolean createAccountBasedAudience) {
        this.createAccountBasedAudience = createAccountBasedAudience;
    }

    public Boolean isCreateContactBasedAudience() {
        return createContactBasedAudience;
    }

    public void setCreateContactBasedAudience(boolean createContactBasedAudience) {
        this.createContactBasedAudience = createContactBasedAudience;
    }

    public Boolean isShowNumAccountsLaunched() {
        return showNumAccountsLaunched;
    }

    public void setShowNumAccountsLaunched(boolean showNumAccountsLaunched) {
        this.showNumAccountsLaunched = showNumAccountsLaunched;
    }

    public Boolean isShowNumAccountsSupressed() {
        return showNumAccountsSupressed;
    }

    public void setShowNumAccountsSupressed(boolean showNumAccountsSupressed) {
        this.showNumAccountsSupressed = showNumAccountsSupressed;
    }

    public Boolean isShowNumAccountsInSegment() {
        return showNumAccountsInSegment;
    }

    public void setShowNumAccountsInSegment(boolean showNumAccountsInSegment) {
        this.showNumAccountsInSegment = showNumAccountsInSegment;
    }

    public Boolean isShowNumContactsLaunched() {
        return showNumContactsLaunched;
    }

    public void setShowNumContactsLaunched(boolean showNumContactsLaunched) {
        this.showNumContactsLaunched = showNumContactsLaunched;
    }

    public Boolean isShowNumContactsSupressed() {
        return showNumContactsSupressed;
    }

    public void setShowNumContactsSupressed(boolean showNumContactsSupressed) {
        this.showNumContactsSupressed = showNumContactsSupressed;
    }

    public Boolean isShowNumContactsInSegment() {
        return showNumContactsInSegment;
    }

    public void setShowNumContactsInSegment(boolean showNumContactsInSegment) {
        this.showNumContactsInSegment = showNumContactsInSegment;
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
        S3ChannelConfig s3ChannelConfig = (S3ChannelConfig) super.copyConfig(config);
        S3ChannelConfig newS3ChannelConfig = (S3ChannelConfig) config;
        s3ChannelConfig.setCreateAccountBasedAudience(newS3ChannelConfig.isCreateAccountBasedAudience());
        s3ChannelConfig.setCreateContactBasedAudience(newS3ChannelConfig.isCreateContactBasedAudience());
        s3ChannelConfig.setShowNumAccountsLaunched(newS3ChannelConfig.isShowNumAccountsLaunched());
        s3ChannelConfig.setShowNumAccountsSupressed(newS3ChannelConfig.isShowNumAccountsSupressed());
        s3ChannelConfig.setShowNumAccountsInSegment(newS3ChannelConfig.isShowNumAccountsInSegment());
        s3ChannelConfig.setShowNumContactsLaunched(newS3ChannelConfig.isShowNumContactsLaunched());
        s3ChannelConfig.setShowNumContactsSupressed(newS3ChannelConfig.isShowNumContactsSupressed());
        s3ChannelConfig.setShowNumContactsInSegment(newS3ChannelConfig.isShowNumContactsInSegment());
        s3ChannelConfig.setSupressAccountWithoutAccountId(newS3ChannelConfig.isSupressAccountWithoutAccountId());
        s3ChannelConfig.setSupressAccountWithoutContacts(newS3ChannelConfig.isSupressAccountWithoutContacts());
        return this;

    }

}
