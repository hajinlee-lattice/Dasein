package com.latticeengines.domain.exposed.serviceflows.datacloud.match;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.datacloud.BaseDataCloudWorkflowConfiguration;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CommitEntityMatchWorkflowConfiguration extends BaseDataCloudWorkflowConfiguration {

    public static class Builder {

        private CommitEntityMatchWorkflowConfiguration configuration = new CommitEntityMatchWorkflowConfiguration();
        private CustomerSpace customerSpace;

        public CommitEntityMatchWorkflowConfiguration.Builder customer(CustomerSpace customerSpace) {
            this.customerSpace = customerSpace;
            return this;
        }

        public CommitEntityMatchWorkflowConfiguration build() {
            configuration.setContainerConfiguration("commitEntityMatchWorkflow", customerSpace,
                    configuration.getClass().getSimpleName());
            return configuration;
        }
    }
}
