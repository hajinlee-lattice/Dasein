package com.latticeengines.domain.exposed.pls.cdl.channel;

import javax.persistence.Transient;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class S3ChannelConfig implements ChannelConfig {

    private static final CDLExternalSystemName systemName = CDLExternalSystemName.AWS_S3;

    @JsonProperty("audienceType")
    private AudienceType audienceType;

    @JsonProperty("accountLimit")
    private Long accountLimit;

    @JsonProperty("s3CampaignExportDir")
    @Transient
    private String s3CampaignExportDir;

    @JsonProperty("isIncludeExportAttributes")
    private Boolean isIncludeExportAttributes = false;

    @JsonProperty("supressAccountsWithoutLookupId")
    private Boolean supressAccountsWithoutLookupId = false;

    @JsonProperty("supressAccountWithoutContacts")
    private Boolean supressAccountWithoutContacts = false;

    public Long getAccountLimit() {
        return accountLimit;
    }

    public void setAccountLimit(Long accountLimit) {
        this.accountLimit = accountLimit;
    }

    public void setAudienceType(AudienceType audienceType) {
        this.audienceType = audienceType;
    }

    public String getS3CampaignExportDir() {
        return s3CampaignExportDir;
    }

    public void setS3CampaignExportDir(String s3CampaignExportDir) {
        this.s3CampaignExportDir = s3CampaignExportDir;
    }

    public Boolean isIncludeExportAttributes() {
        return isIncludeExportAttributes;
    }

    public void setIsIncludeExportAttributes(boolean isIncludeExportAttributes) {
        this.isIncludeExportAttributes = isIncludeExportAttributes;
    }

    public Boolean isSupressAccountsWithoutLookupId() {
        return supressAccountsWithoutLookupId;
    }

    public void setSupressAccountsWithoutLookupId(boolean supressAccountsWithoutLookupId) {
        this.supressAccountsWithoutLookupId = supressAccountsWithoutLookupId;
    }

    public Boolean isSupressAccountWithoutContacts() {
        return supressAccountWithoutContacts;
    }

    public void setSupressAccountWithoutContacts(boolean supressAccountWithoutContacts) {
        this.supressAccountWithoutContacts = supressAccountWithoutContacts;
    }

    @Override
    public CDLExternalSystemName getSystemName() {
        return systemName;
    }

    @Override
    public AudienceType getAudienceType() {
        return AudienceType.ACCOUNTS;
    }

    @Override
    public ChannelConfig copyConfig(ChannelConfig config) {
        S3ChannelConfig s3ChannelConfig = this;
        S3ChannelConfig newS3ChannelConfig = (S3ChannelConfig) config;
        s3ChannelConfig.setAudienceType(newS3ChannelConfig.getAudienceType());
        s3ChannelConfig.setAccountLimit(newS3ChannelConfig.getAccountLimit());
        s3ChannelConfig.setS3CampaignExportDir(newS3ChannelConfig.getS3CampaignExportDir());
        s3ChannelConfig.setIsIncludeExportAttributes(newS3ChannelConfig.isIncludeExportAttributes());
        s3ChannelConfig.setSupressAccountsWithoutLookupId(newS3ChannelConfig.isSupressAccountsWithoutLookupId());
        s3ChannelConfig.setSupressAccountWithoutContacts(newS3ChannelConfig.isSupressAccountWithoutContacts());
        return this;
    }
}
