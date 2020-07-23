package com.latticeengines.domain.exposed.serviceflows.cdl.migrate;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ImportDynamoTableFromS3Configuration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.BaseExportToDynamoConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportTimelineRawTableToDynamoStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToDynamoStepConfiguration;

public class MigrateDynamoWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static final String IMPORT_TABLE_NAMES = "IMPORT_TABLE_NAMES";

    public static class Builder {
        private MigrateDynamoWorkflowConfiguration configuration = new MigrateDynamoWorkflowConfiguration();

        private ImportDynamoTableFromS3Configuration importDynamoTableFromS3 = new ImportDynamoTableFromS3Configuration();

        private ExportToDynamoStepConfiguration exportToDynamo = new ExportToDynamoStepConfiguration();
        private ExportTimelineRawTableToDynamoStepConfiguration exportTimelineRawTableToDynamoStepConfiguration =
                new ExportTimelineRawTableToDynamoStepConfiguration();
        private Map<String, ? extends BaseExportToDynamoConfiguration> exportSteps = Lists.newArrayList(exportToDynamo, exportTimelineRawTableToDynamoStepConfiguration)
                .stream().collect(Collectors.toMap(
                        stepConfiguration -> stepConfiguration.getEntityClass().getCanonicalName(), stepConfiguration -> stepConfiguration));

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("migrateDynamoWorkflow", customerSpace, configuration.getClass().getSimpleName());
            importDynamoTableFromS3.setCustomerSpace(customerSpace);
            exportToDynamo.setCustomerSpace(customerSpace);
            exportTimelineRawTableToDynamoStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder tableNames(List<String> tableNames) {
            importDynamoTableFromS3.setTableNames(tableNames);
            return this;
        }

        public Builder setContextKey(String entityClass) {
            for (Map.Entry<String, ? extends BaseExportToDynamoConfiguration> entry : exportSteps.entrySet()) {
                String key = entry.getKey();
                BaseExportToDynamoConfiguration value = entry.getValue();
                if (!key.equals(entityClass)) {
                    value.setSkipStep(true);
                } else {
                    importDynamoTableFromS3.setContextKey(value.getContextKey());
                }
            }
            return this;
        }

        public Builder dynamoSignature(String signature) {
            importDynamoTableFromS3.setDynamoSignature(signature);
            for (Map.Entry<String, ? extends BaseExportToDynamoConfiguration> entry : exportSteps.entrySet()) {
                BaseExportToDynamoConfiguration value = entry.getValue();
                if (value.getContextKey().equals(importDynamoTableFromS3.getContextKey())) {
                    value.setDynamoSignature(signature);
                }
            }
            return this;
        }

        public Builder migrateTable(Boolean migrateTable) {
            for (Map.Entry<String, ? extends BaseExportToDynamoConfiguration> entry : exportSteps.entrySet()) {
                BaseExportToDynamoConfiguration value = entry.getValue();
                if (value.getContextKey().equals(importDynamoTableFromS3.getContextKey())) {
                    value.setMigrateTable(migrateTable);
                    if (value instanceof ExportTimelineRawTableToDynamoStepConfiguration) {
                        ((ExportTimelineRawTableToDynamoStepConfiguration) entry.getValue()).setRegisterDataUnit(true);
                    }
                }
            }
            return this;
        }

        public MigrateDynamoWorkflowConfiguration build() {
            configuration.add(importDynamoTableFromS3);
            configuration.add(exportToDynamo);
            configuration.add(exportTimelineRawTableToDynamoStepConfiguration);
            return configuration;
        }
    }
}
