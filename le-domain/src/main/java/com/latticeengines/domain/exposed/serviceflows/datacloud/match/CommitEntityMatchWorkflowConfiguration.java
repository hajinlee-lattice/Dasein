package com.latticeengines.domain.exposed.serviceflows.datacloud.match;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.datacloud.BaseDataCloudWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.CommitEntityMatchConfiguration;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CommitEntityMatchWorkflowConfiguration extends BaseDataCloudWorkflowConfiguration {

    public static class Builder {

        private CommitEntityMatchWorkflowConfiguration configuration = new CommitEntityMatchWorkflowConfiguration();
        private CommitEntityMatchConfiguration commitEntity = new CommitEntityMatchConfiguration();

        public CommitEntityMatchWorkflowConfiguration.Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            commitEntity.setCustomerSpace(customerSpace);
            return this;
        }

        public CommitEntityMatchWorkflowConfiguration.Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public CommitEntityMatchWorkflowConfiguration.Builder entitySet(Set<String> entitySet) {
            commitEntity.setEntitySet(entitySet);
            return this;
        }

        public CommitEntityMatchWorkflowConfiguration build() {
            configuration.setContainerConfiguration("commitEntityMatchWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(commitEntity);
            return configuration;
        }
    }
}
