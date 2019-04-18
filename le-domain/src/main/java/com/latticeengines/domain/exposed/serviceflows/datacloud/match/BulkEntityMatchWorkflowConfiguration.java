package com.latticeengines.domain.exposed.serviceflows.datacloud.match;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishRequest;
import com.latticeengines.domain.exposed.serviceflows.datacloud.BaseDataCloudWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.CleanupBulkEntityMatchConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.PrepareBulkEntityMatchConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.PublishEntityConfiguration;

/**
 * Configuration class for BulkEntityMatchWorkflow
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BulkEntityMatchWorkflowConfiguration extends BaseDataCloudWorkflowConfiguration {

    private boolean publishBeforeMatch;

    private boolean performBulkMatch;

    private boolean publishAfterMatch;

    @JsonProperty("publish_before_match")
    public boolean shouldPublishBeforeMatch() {
        return publishBeforeMatch;
    }

    @JsonProperty("perform_bulk_match")
    public boolean shouldPerformBulkMatch() {
        return performBulkMatch;
    }

    @JsonProperty("publish_after_match")
    public boolean shouldPublishAfterMatch() {
        return publishAfterMatch;
    }

    @JsonProperty("publish_before_match")
    public void setPublishBeforeMatch(boolean publishBeforeMatch) {
        this.publishBeforeMatch = publishBeforeMatch;
    }

    @JsonProperty("perform_bulk_match")
    public void setPerformBulkMatch(boolean performBulkMatch) {
        this.performBulkMatch = performBulkMatch;
    }

    @JsonProperty("publish_after_match")
    public void setPublishAfterMatch(boolean publishAfterMatch) {
        this.publishAfterMatch = publishAfterMatch;
    }

    public static class Builder {
        private String entity;
        private CustomerSpace customerSpace;
        // customer spaces for bumping up version
        private Map<EntityMatchEnvironment, List<String>> customerSpaceMap = new HashMap<>();
        private PrepareBulkEntityMatchConfiguration prepare = new PrepareBulkEntityMatchConfiguration();
        private CleanupBulkEntityMatchConfiguration cleanup = new CleanupBulkEntityMatchConfiguration();
        private PublishEntityConfiguration beforeMatchPublish;
        private BulkMatchWorkflowConfiguration bulkMatch;
        private PublishEntityConfiguration afterMatchPublish;

        public Builder(@NotNull String entity, @NotNull CustomerSpace customerSpace) {
            Preconditions.checkNotNull(entity);
            Preconditions.checkNotNull(customerSpace);
            this.entity = entity;
            this.customerSpace = customerSpace;
        }

        public Builder copyTestFile(@NotNull String bucket, @NotNull List<String> testFilePaths, @NotNull String destDir) {
            prepare.setSrcBucket(bucket);
            prepare.setSrcTestFilePath(testFilePaths);
            prepare.setDestTestDirectory(destDir);
            return this;
        }

        public Builder podId(String podId) {
            cleanup.setPodId(podId);
            return this;
        }

        public Builder rootOperationUid(String rootOperationUid) {
            cleanup.setRootOperationUid(rootOperationUid);
            return this;
        }

        public Builder bulkMatch(BulkMatchWorkflowConfiguration bulkMatch) {
            this.bulkMatch = bulkMatch;
            return this;
        }

        public Builder bumpUpVersion(EntityMatchEnvironment env, String... customerSpaces) {
            customerSpaceMap.put(env, Arrays.asList(customerSpaces));
            return this;
        }

        public Builder publishBeforeMatch(EntityPublishRequest request) {
            // cannot call this method multiple times
            Preconditions.checkArgument(beforeMatchPublish == null);
            beforeMatchPublish = new PublishEntityConfiguration();
            beforeMatchPublish.setEntityPublishRequest(request);
            return this;
        }

        public Builder publishAfterMatch(EntityPublishRequest request) {
            // cannot call this method multiple times
            Preconditions.checkArgument(afterMatchPublish == null);
            afterMatchPublish = new PublishEntityConfiguration();
            afterMatchPublish.setEntityPublishRequest(request);
            return this;
        }

        public Builder cleanup(String destDir) {
            cleanup.setTmpDir(destDir);
            return this;
        }

        public BulkEntityMatchWorkflowConfiguration build() {
            BulkEntityMatchWorkflowConfiguration config = new BulkEntityMatchWorkflowConfiguration();
            config.setPerformBulkMatch(bulkMatch != null);
            config.setContainerConfiguration("bulkEntityMatchWorkflow", customerSpace,
                    config.getClass().getSimpleName());
            prepare.setCustomerSpacesToBumpUpVersion(customerSpaceMap);
            config.add(prepare);
            // optional steps
            if (beforeMatchPublish != null) {
                config.add(beforeMatchPublish);
                config.setPublishBeforeMatch(true);
            }
            if (bulkMatch != null) {
                config.add(bulkMatch);
                config.setPerformBulkMatch(true);
            }
            if (afterMatchPublish != null) {
                config.add(afterMatchPublish);
                config.setPublishAfterMatch(true);
            }
            config.add(cleanup);
            return config;
        }
    }
}
