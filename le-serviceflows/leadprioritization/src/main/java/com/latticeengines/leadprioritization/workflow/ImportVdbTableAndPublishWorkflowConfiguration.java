package com.latticeengines.leadprioritization.workflow;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.VdbCreateTableRule;
import com.latticeengines.domain.exposed.pls.VdbSpecMetadata;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.importvdbtable.ImportVdbTableStepConfiguration;

import java.util.List;

public class ImportVdbTableAndPublishWorkflowConfiguration extends WorkflowConfiguration {

    private ImportVdbTableAndPublishWorkflowConfiguration() {
    }

    public static class Builder {
        private ImportVdbTableAndPublishWorkflowConfiguration configuration = new
                ImportVdbTableAndPublishWorkflowConfiguration();

        private ImportVdbTableStepConfiguration importVdbTableConfiguration = new ImportVdbTableStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("importVdbTableAndPublishWorkflow", customerSpace,
                    "importVdbTableAndPublishWorkflow");
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
