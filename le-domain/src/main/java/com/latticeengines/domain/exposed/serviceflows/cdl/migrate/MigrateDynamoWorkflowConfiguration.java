package com.latticeengines.domain.exposed.serviceflows.cdl.migrate;

import java.util.List;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ImportDynamoTableFromS3Configuration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToDynamoStepConfiguration;

public class MigrateDynamoWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static final String IMPORT_TABLE_NAMES = "IMPORT_TABLE_NAMES";

    public static class Builder {
        private MigrateDynamoWorkflowConfiguration configuration = new MigrateDynamoWorkflowConfiguration();

        private ImportDynamoTableFromS3Configuration importDynamoTableFromS3 = new ImportDynamoTableFromS3Configuration();
        private ExportToDynamoStepConfiguration exportToDynamo = new ExportToDynamoStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("migrateDynamoWorkflow", customerSpace, configuration.getClass().getSimpleName());
            importDynamoTableFromS3.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder tableNames(List<String> tableNames) {
            importDynamoTableFromS3.setTableNames(tableNames);
            return this;
        }

        public Builder onlyUpdateSignature(Boolean onlyUpdateSignature) {
            exportToDynamo.setOnlyUpdateSignature(onlyUpdateSignature);
            return this;
        }

        public MigrateDynamoWorkflowConfiguration build() {
            configuration.add(importDynamoTableFromS3);
            configuration.add(exportToDynamo);
            return configuration;
        }
    }
}
