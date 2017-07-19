package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.ExportDataToRedshiftConfiguration;

public class RedshiftPublishWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    private RedshiftPublishWorkflowConfiguration() {
    }

    public static class Builder {
        private RedshiftPublishWorkflowConfiguration configuration = new RedshiftPublishWorkflowConfiguration();

        private ExportDataToRedshiftConfiguration exportDataToRedshiftConfiguration = new ExportDataToRedshiftConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("redshiftPublishWorkflow", customerSpace,
                    "redshiftPublishWorkflow");
            exportDataToRedshiftConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            exportDataToRedshiftConfiguration.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder hdfsToRedshiftConfiguration(HdfsToRedshiftConfiguration hdfsToRedshiftConfiguration) {
            exportDataToRedshiftConfiguration.setHdfsToRedshiftConfiguration(hdfsToRedshiftConfiguration);
            return this;
        }

        public Builder sourceTables(Map<BusinessEntity, Table> sourceTables) {
            exportDataToRedshiftConfiguration.setSourceTables(sourceTables);
            return this;
        }

        // mainly for test
        public Builder enforceTargetTableName(String targetTableName) {
            exportDataToRedshiftConfiguration.setTargetTableName(targetTableName);
            return this;
        }

        public RedshiftPublishWorkflowConfiguration build() {
            configuration.add(exportDataToRedshiftConfiguration);
            return configuration;
        }
    }
}
