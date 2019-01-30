package com.latticeengines.testframework.exposed.domain;

import java.util.Map;
import java.util.Set;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.RatingBucketName;

public class PlayLaunchConfig {

    private String existingTenant;

    private boolean mockRatingTable;

    private boolean testPlayCrud;

    private CDLExternalSystemType destinationSystemType;

    private String destinationSystemId;

    private boolean launchPlay;

    private Set<RatingBucketName> bucketsToLaunch;

    private Long topNCount;

    private boolean excludeItemsWithoutSalesforceId;

    private boolean playLaunchDryRun;

    private Map<String, Boolean> featureFlags;

    private PlayLaunchConfig() {
        testPlayCrud = true;
    }

    public static class Builder {

        private PlayLaunchConfig playLaunchConfig = new PlayLaunchConfig();

        public Builder existingTenant(String existingTenant) {
            playLaunchConfig.existingTenant = existingTenant;
            return this;
        }

        public Builder mockRatingTable(boolean mockRatingTable) {
            playLaunchConfig.mockRatingTable = mockRatingTable;
            return this;
        }

        public Builder testPlayCrud(boolean testPlayCrud) {
            playLaunchConfig.testPlayCrud = testPlayCrud;
            return this;
        }

        public Builder destinationSystemType(CDLExternalSystemType destinationSystemType) {
            playLaunchConfig.destinationSystemType = destinationSystemType;
            return this;
        }

        public Builder destinationSystemId(String destinationSystemId) {
            playLaunchConfig.destinationSystemId = destinationSystemId;
            return this;
        }

        public Builder launchPlay(boolean launchPlay) {
            playLaunchConfig.launchPlay = launchPlay;
            return this;
        }

        public Builder bucketsToLaunch(Set<RatingBucketName> bucketsToLaunch) {
            playLaunchConfig.bucketsToLaunch = bucketsToLaunch;
            return this;
        }

        public Builder topNCount(Long topNCount) {
            playLaunchConfig.topNCount = topNCount;
            return this;
        }

        public Builder playLaunchDryRun(boolean playLaunchDryRun) {
            playLaunchConfig.playLaunchDryRun = playLaunchDryRun;
            return this;
        }

        public Builder excludeItemsWithoutSalesforceId(boolean excludeItemsWithoutSalesforceId) {
            playLaunchConfig.excludeItemsWithoutSalesforceId = excludeItemsWithoutSalesforceId;
            return this;
        }

        public Builder featureFlags(Map<String, Boolean> featureFlags) {
            playLaunchConfig.featureFlags = featureFlags;
            return this;
        }

        public PlayLaunchConfig build() {
            return playLaunchConfig;
        }
    }

    public String getExistingTenant() {
        return existingTenant;
    }

    public boolean isMockRatingTable() {
        return mockRatingTable;
    }

    public boolean isTestPlayCrud() {
        return testPlayCrud;
    }

    public CDLExternalSystemType getDestinationSystemType() {
        return destinationSystemType;
    }

    public String getDestinationSystemId() {
        return destinationSystemId;
    }

    public boolean isLaunchPlay() {
        return launchPlay;
    }

    public Set<RatingBucketName> getBucketsToLaunch() {
        return bucketsToLaunch;
    }

    public Long getTopNCount() {
        return topNCount;
    }

    public boolean isExcludeItemsWithoutSalesforceId() {
        return excludeItemsWithoutSalesforceId;
    }

    public boolean isPlayLaunchDryRun() {
        return playLaunchDryRun;
    }

    public Map<String, Boolean> getFeatureFlags() {
        return featureFlags;
    }
}