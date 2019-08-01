package com.latticeengines.testframework.exposed.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;

public class TestPlaySetupConfig {

    private String existingTenantName;

    private boolean mockRatingTable;

    private boolean testPlayCrud;

    private boolean launchPlay;

    private boolean playLaunchDryRun;

    private List<TestPlayChannelConfig> channels = new ArrayList<>();

    private Map<String, Boolean> featureFlags;

    private TestPlaySetupConfig() {
        testPlayCrud = true;
    }

    public static class Builder {

        private TestPlaySetupConfig testPlaySetupConfig = new TestPlaySetupConfig();

        public Builder existingTenant(String existingTenant) {
            testPlaySetupConfig.existingTenantName = existingTenant;
            return this;
        }

        public Builder mockRatingTable(boolean mockRatingTable) {
            testPlaySetupConfig.mockRatingTable = mockRatingTable;
            return this;
        }

        public Builder testPlayCrud(boolean testPlayCrud) {
            testPlaySetupConfig.testPlayCrud = testPlayCrud;
            return this;
        }

        public Builder launchPlay(boolean launchPlay) {
            testPlaySetupConfig.launchPlay = launchPlay;
            return this;
        }

        public Builder playLaunchDryRun(boolean playLaunchDryRun) {
            testPlaySetupConfig.playLaunchDryRun = playLaunchDryRun;
            return this;
        }

        public Builder featureFlags(Map<String, Boolean> featureFlags) {
            testPlaySetupConfig.featureFlags = featureFlags;
            return this;
        }

        public Builder addChannel(TestPlayChannelConfig channelConfig) {
            if (CollectionUtils.isEmpty(testPlaySetupConfig.channels)) {
                testPlaySetupConfig.channels = new ArrayList<>();
            }
            testPlaySetupConfig.channels.add(channelConfig);
            return this;
        }

        public TestPlaySetupConfig build() {
            return testPlaySetupConfig;
        }
    }

    public boolean isLaunchPlay() {
        return launchPlay;
    }

    public boolean isPlayLaunchDryRun() {
        return playLaunchDryRun;
    }

    public Map<String, Boolean> getFeatureFlags() {
        return featureFlags;
    }

    public String getExistingTenantName() {
        return existingTenantName;
    }

    public boolean isMockRatingTable() {
        return mockRatingTable;
    }

    public boolean isTestPlayCrud() {
        return testPlayCrud;
    }

    public List<TestPlayChannelConfig> getChannels() {
        return channels;
    }

    // Convenience method if the test only uses one channel
    public TestPlayChannelConfig getSinglePlayLaunchChannelConfig() {
        return CollectionUtils.isNotEmpty(channels) ? channels.get(0) : null;
    }

}
