package com.latticeengines.domain.exposed.serviceflows.leadprioritization;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportVdbTableStepConfiguration;

public class ImportVdbTableAndPublishWorkflowConfiguration extends BaseLPWorkflowConfiguration {

    private ImportVdbTableAndPublishWorkflowConfiguration() {
    }

    public static class Builder {
        private ImportVdbTableAndPublishWorkflowConfiguration configuration = new ImportVdbTableAndPublishWorkflowConfiguration();

        private ImportVdbTableStepConfiguration importVdbTableConfiguration = new ImportVdbTableStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("importVdbTableAndPublishWorkflow",
                    customerSpace, configuration.getClass().getSimpleName());
            importVdbTableConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder collectionIdentifier(String collectionIdentifier) {
            importVdbTableConfiguration.setCollectionIdentifier(collectionIdentifier);
            return this;
        }

        public Builder importConfigurationStr(String importConfigurationStr) {
            importVdbTableConfiguration.setImportConfigurationStr(importConfigurationStr);
            return this;
        }

        public ImportVdbTableAndPublishWorkflowConfiguration build() {
            configuration.add(importVdbTableConfiguration);
            return configuration;
        }

    }
}
