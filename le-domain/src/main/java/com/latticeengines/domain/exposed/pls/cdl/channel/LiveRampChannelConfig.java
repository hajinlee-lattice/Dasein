package com.latticeengines.domain.exposed.pls.cdl.channel;

import java.util.Arrays;
import java.util.HashSet;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

public abstract class LiveRampChannelConfig implements ChannelConfig {
    private static final AudienceType audienceType = AudienceType.ACCOUNTS;

    @JsonProperty("accountLimit")
    private Long accountLimit;

    @JsonProperty("audienceId")
    private String audienceId;

    @JsonProperty("audienceName")
    private String audienceName;

    @JsonProperty("jobLevels")
    private String[] jobLevels;

    @JsonProperty("jobFunctions")
    private String[] jobFunctions;

    public Long getAccountLimit() {
        return accountLimit;
    }

    public void setAccountLimit(Long accountLimit) {
        this.accountLimit = accountLimit;
    }

    @Override
    public boolean isSuppressContactsWithoutEmails() {
        return false;
    }

    @Override
    public boolean isSuppressAccountsWithoutContacts() {
        return false;
    }

    @Override
    public boolean isSuppressAccountsWithoutLookupId() {
        return false;
    }

    @Override
    public String getAudienceId() {
        return audienceId;
    }

    @Override
    public void setAudienceId(String audienceId) {
        this.audienceId = audienceId;
    }

    @Override
    public String getAudienceName() {
        return audienceName;
    }

    @Override
    public void setAudienceName(String audienceName) {
        this.audienceName = audienceName;
    }

    @Override
    public AudienceType getAudienceType() {
        return audienceType;
    }

    public String[] getJobLevels() {
        return jobLevels;
    }

    public void setJobLevels(String[] jobLevels) {
        this.jobLevels = jobLevels;
    }

    public String[] getJobFunctions() {
        return jobFunctions;
    }

    public void setJobFunctions(String[] jobFunction) {
        this.jobFunctions = jobFunction;
    }

    private boolean arraysEqual(String[] array1, String[] array2) {
        HashSet<String> hashSet1 = new HashSet<>();
        HashSet<String> hashSet2 = new HashSet<>();

        if (array1 != null) {
            hashSet1.addAll(Arrays.asList(array1));
        }
        if (array2 != null) {
            hashSet2.addAll(Arrays.asList(array2));
        }

        return hashSet1.equals(hashSet2);
    }

    @Override
    public boolean shouldResetDeltaCalculations(ChannelConfig channelConfig) {
        LiveRampChannelConfig updatedConfig = (LiveRampChannelConfig) channelConfig;

        boolean jobLevelsEqual = arraysEqual(this.jobLevels, updatedConfig.jobLevels);

        boolean jobFunctionsEqual = arraysEqual(this.jobFunctions, updatedConfig.jobFunctions);

        return StringUtils.isBlank(this.audienceName) ? StringUtils.isNotBlank(updatedConfig.audienceName) //
                : !(this.audienceName.equals(updatedConfig.audienceName) && jobLevelsEqual && jobFunctionsEqual);
    }

    @Override
    public void populateLaunchFromChannelConfig(PlayLaunch playLaunch) {
        playLaunch.setAudienceId(this.getAudienceId());
        playLaunch.setAudienceName(this.getAudienceName());
    }
}
