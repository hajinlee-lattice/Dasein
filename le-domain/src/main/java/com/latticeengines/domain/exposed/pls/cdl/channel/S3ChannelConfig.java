package com.latticeengines.domain.exposed.pls.cdl.channel;

import javax.persistence.Transient;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

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

    @JsonProperty("includeExportAttributes")
    private boolean includeExportAttributes = false;

    public Long getAccountLimit() { return accountLimit; }

    public void setAccountLimit(Long accountLimit) { this.accountLimit = accountLimit; }

    @Override
    public String getAudienceId() { return ""; }

    @Override
    public void setAudienceId(String audienceId) { }

    @Override
    public String getAudienceName() { return ""; }

    @Override
    public void setAudienceName(String audienceName) { }

    public void setAudienceType(AudienceType audienceType) { this.audienceType = audienceType; }

    public String getS3CampaignExportDir() { return s3CampaignExportDir; }

    public void setS3CampaignExportDir(String s3CampaignExportDir) { this.s3CampaignExportDir = s3CampaignExportDir; }

    public boolean isIncludeExportAttributes() { return includeExportAttributes; }

    public void setIncludeExportAttributes(boolean includeExportAttributes) {
        this.includeExportAttributes = includeExportAttributes;
    }

    @Override
    @JsonProperty("suppressAccountsWithoutContacts")
    public boolean isSuppressAccountsWithoutContacts() { return false; }

    @Override
    @JsonProperty("suppressContactsWithoutEmails")
    public boolean isSuppressContactsWithoutEmails() { return false; }

    @Override
    @JsonProperty("suppressAccountsWithoutLookupId")
    public boolean isSuppressAccountsWithoutLookupId() { return false; }

    @Override
    public CDLExternalSystemName getSystemName() { return systemName; }

    @Override
    @JsonProperty("audienceType")
    public AudienceType getAudienceType() { return audienceType; }

    @Override
    public boolean shouldResetDeltaCalculations(ChannelConfig channelConfig) { return false; }

    @Override
    public void populateLaunchFromChannelConfig(PlayLaunch playLaunch) {
        // No special Launch properties to set in Play launch for S3
    }

    @Override
    public ChannelConfig copyConfig(ChannelConfig config) {
        S3ChannelConfig s3ChannelConfig = this;
        S3ChannelConfig newS3ChannelConfig = (S3ChannelConfig) config;
        s3ChannelConfig.setAudienceType(newS3ChannelConfig.getAudienceType());
        s3ChannelConfig.setAccountLimit(newS3ChannelConfig.getAccountLimit());
        s3ChannelConfig.setS3CampaignExportDir(newS3ChannelConfig.getS3CampaignExportDir());
        s3ChannelConfig.setIncludeExportAttributes(newS3ChannelConfig.isIncludeExportAttributes());
        return this;
    }

}
