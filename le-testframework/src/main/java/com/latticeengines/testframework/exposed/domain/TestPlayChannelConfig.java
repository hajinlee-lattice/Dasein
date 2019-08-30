package com.latticeengines.testframework.exposed.domain;

import java.util.Set;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.LaunchType;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;

public class TestPlayChannelConfig {

    private CDLExternalSystemType destinationSystemType;

    private String destinationSystemId;

    private CDLExternalSystemName destinationSystemName;

    private Set<RatingBucketName> bucketsToLaunch;

    private Long topNCount;

    private String audienceId;

    private AudienceType audienceType;

    private String trayAuthenticationId;

    private boolean excludeItemsWithoutSalesforceId;

    private boolean alwaysOn;

    private String cronSchedule;

    private LaunchType launchType;

    public static class Builder {
        private TestPlayChannelConfig testPlayChannelSetupConfig = new TestPlayChannelConfig();

        public TestPlayChannelConfig.Builder destinationSystemType(CDLExternalSystemType destinationSystemType) {
            testPlayChannelSetupConfig.destinationSystemType = destinationSystemType;
            return this;
        }

        public TestPlayChannelConfig.Builder destinationSystemName(CDLExternalSystemName destinationSystemName) {
            testPlayChannelSetupConfig.destinationSystemName = destinationSystemName;
            return this;
        }

        public TestPlayChannelConfig.Builder destinationSystemId(String destinationSystemId) {
            testPlayChannelSetupConfig.destinationSystemId = destinationSystemId;
            return this;
        }

        public TestPlayChannelConfig.Builder bucketsToLaunch(Set<RatingBucketName> bucketsToLaunch) {
            testPlayChannelSetupConfig.bucketsToLaunch = bucketsToLaunch;
            return this;
        }

        public TestPlayChannelConfig.Builder topNCount(Long topNCount) {
            testPlayChannelSetupConfig.topNCount = topNCount;
            return this;
        }

        public TestPlayChannelConfig.Builder excludeItemsWithoutSalesforceId(boolean excludeItemsWithoutSalesforceId) {
            testPlayChannelSetupConfig.excludeItemsWithoutSalesforceId = excludeItemsWithoutSalesforceId;
            return this;
        }

        public TestPlayChannelConfig.Builder audienceId(String audienceId) {
            testPlayChannelSetupConfig.audienceId = audienceId;
            return this;
        }

        public TestPlayChannelConfig.Builder audienceType(AudienceType audienceType) {
            testPlayChannelSetupConfig.audienceType = audienceType;
            return this;
        }

        public TestPlayChannelConfig.Builder isAlwaysOn(boolean alwaysOn) {
            testPlayChannelSetupConfig.alwaysOn = alwaysOn;
            return this;
        }

        public TestPlayChannelConfig.Builder cronSchedule(String cronSchedule) {
            testPlayChannelSetupConfig.cronSchedule = cronSchedule;
            return this;
        }

        public TestPlayChannelConfig.Builder launchType(LaunchType launchType) {
            testPlayChannelSetupConfig.launchType = launchType;
            return this;
        }

        public TestPlayChannelConfig.Builder trayAuthenticationId(String authId) {
            testPlayChannelSetupConfig.trayAuthenticationId = authId;
            return this;
        }

        public TestPlayChannelConfig build() {
            return testPlayChannelSetupConfig;
        }
    }

    public CDLExternalSystemType getDestinationSystemType() {
        return destinationSystemType;
    }

    public CDLExternalSystemName getDestinationSystemName() {
        return destinationSystemName;
    }

    public String getDestinationSystemId() {
        return destinationSystemId;
    }

    public Set<RatingBucketName> getBucketsToLaunch() {
        return bucketsToLaunch;
    }

    public LaunchType getLaunchType() {
        return launchType;
    }

    public Long getTopNCount() {
        return topNCount;
    }

    public boolean isExcludeItemsWithoutSalesforceId() {
        return excludeItemsWithoutSalesforceId;
    }

    public String getAudienceId() {
        return audienceId;
    }

    public boolean isAlwaysOn() {
        return alwaysOn;
    }

    public String getCronSchedule() {
        return cronSchedule;
    }

    public void setAudienceId(String audienceId) {
        this.audienceId = audienceId;
    }

    public AudienceType getAudienceType() {
        return audienceType;
    }

    public void setAudienceType(AudienceType audienceType) {
        this.audienceType = audienceType;
    }

    public String getTrayAuthenticationId() {
        return trayAuthenticationId;
    }

    public void setTrayAuthenticationId(String trayAuthenticationId) {
        this.trayAuthenticationId = trayAuthenticationId;
    }

}
