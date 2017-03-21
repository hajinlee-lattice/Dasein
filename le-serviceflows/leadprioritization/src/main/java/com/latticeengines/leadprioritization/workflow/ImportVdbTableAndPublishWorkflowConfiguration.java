package com.latticeengines.leadprioritization.workflow;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
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

        public Builder dataCatagory(String dataCatagory) {
            importVdbTableConfiguration.setDataCategory(dataCatagory);
            return this;
        }

        public Builder tableName(String tableName) {
            importVdbTableConfiguration.setTableName(tableName);
            return this;
        }

        public Builder extractIdentifier(String extractIdentifier) {
            importVdbTableConfiguration.setExtractIdentifier(extractIdentifier);
            return this;
        }

        public Builder getQueryDataEndpoint(String getQueryDataEndpoint) {
            importVdbTableConfiguration.setGetQueryDataEndpoint(getQueryDataEndpoint);
            return this;
        }

        public Builder vdbQueryHandle(String vdbQueryHandle) {
            importVdbTableConfiguration.setVdbQueryHandle(vdbQueryHandle);
            return this;
        }

        public Builder totalRows(int totalRows) {
            importVdbTableConfiguration.setTotalRows(totalRows);
            return this;
        }

        public Builder batchSize(int batchSize) {
            importVdbTableConfiguration.setBatchSize(batchSize);
            return this;
        }

        public Builder reportStatusEndpoint(String reportStatusEndpoint) {
            importVdbTableConfiguration.setReportStatusEndpoint(reportStatusEndpoint);
            return this;
        }

        public Builder metadataList(List<VdbSpecMetadata> metadataList) {
            importVdbTableConfiguration.setMetadataList(metadataList);
            return this;
        }

        public Builder mergeRule(String mergeRule) {
            importVdbTableConfiguration.setMergeRule(mergeRule);
            return this;
        }

        public Builder createTableRule(VdbCreateTableRule createTableRule) {
            importVdbTableConfiguration.setCreateTableRule(createTableRule);
            return this;
        }

        public ImportVdbTableAndPublishWorkflowConfiguration build() {
            configuration.add(importVdbTableConfiguration);
            return configuration;
        }

    }
}
