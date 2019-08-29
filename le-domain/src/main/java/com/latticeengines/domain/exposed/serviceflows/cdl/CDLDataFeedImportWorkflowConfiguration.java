package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ImportDataFeedTaskConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ImportDataTableFromS3Configuration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.PrepareImportConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.InputFileValidatorConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;

public class CDLDataFeedImportWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public CDLDataFeedImportWorkflowConfiguration() {
    }

    public static class Builder {
        private CDLDataFeedImportWorkflowConfiguration configuration = new CDLDataFeedImportWorkflowConfiguration();

        private ImportDataFeedTaskConfiguration importDataFeedTaskConfiguration = new ImportDataFeedTaskConfiguration();
        private InputFileValidatorConfiguration inputFileValidatorConfiguration = new InputFileValidatorConfiguration();
        private ImportExportS3StepConfiguration exportToS3 = new ImportExportS3StepConfiguration();
        private PrepareImportConfiguration prepareImportConfiguration = new PrepareImportConfiguration();
        private ImportDataTableFromS3Configuration importFromS3 = new ImportDataTableFromS3Configuration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("cdlDataFeedImportWorkflow", customerSpace,
                    configuration.getClass().getSimpleName());
            importDataFeedTaskConfiguration.setCustomerSpace(customerSpace);
            exportToS3.setCustomerSpace(customerSpace);
            inputFileValidatorConfiguration.setCustomerSpace(customerSpace);
            prepareImportConfiguration.setCustomerSpace(customerSpace);
            importFromS3.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            importDataFeedTaskConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            exportToS3.setInternalResourceHostPort(internalResourceHostPort);
            inputFileValidatorConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            prepareImportConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            importFromS3.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder dataFeedTaskId(String dataFeedTaskId) {
            importDataFeedTaskConfiguration.setDataFeedTaskId(dataFeedTaskId);
            inputFileValidatorConfiguration.setDataFeedTaskId(dataFeedTaskId);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            importDataFeedTaskConfiguration.setMicroServiceHostPort(microServiceHostPort);
            exportToS3.setMicroServiceHostPort(microServiceHostPort);
            inputFileValidatorConfiguration.setMicroServiceHostPort(microServiceHostPort);
            prepareImportConfiguration.setMicroServiceHostPort(microServiceHostPort);
            importFromS3.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder importConfig(String importConfig) {
            importDataFeedTaskConfiguration.setImportConfig(importConfig);
            return this;
        }

        public Builder prepareImportConfig(PrepareImportConfiguration prepareImportConfig) {
            if (prepareImportConfig != null) {
                prepareImportConfiguration.setSourceBucket(prepareImportConfig.getSourceBucket());
                prepareImportConfiguration.setSourceKey(prepareImportConfig.getSourceKey());
                prepareImportConfiguration.setDestBucket(prepareImportConfig.getDestBucket());
                prepareImportConfiguration.setDestKey(prepareImportConfig.getDestKey());
                prepareImportConfiguration.setBackupKey(prepareImportConfig.getBackupKey());
                prepareImportConfiguration.setDataFeedTaskId(prepareImportConfig.getDataFeedTaskId());
                prepareImportConfiguration.setEmailInfo(prepareImportConfig.getEmailInfo());
            } else {
                prepareImportConfiguration.setSkipStep(true);
            }
            return this;
        }

        public Builder userId(String userId) {
            configuration.setUserId(userId);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public CDLDataFeedImportWorkflowConfiguration build() {
            configuration.add(prepareImportConfiguration);
            configuration.add(importDataFeedTaskConfiguration);
            configuration.add(exportToS3);
            configuration.add(inputFileValidatorConfiguration);
            configuration.add(importFromS3);
            return configuration;
        }

        // special case for entity match
        public Builder fileValidation(BusinessEntity businessEntity, boolean enableEntityMatch,
                                      boolean enableEntityMatchGA) {
            inputFileValidatorConfiguration.setEntity(businessEntity);
            inputFileValidatorConfiguration.setEnableEntityMatch(enableEntityMatch);
            inputFileValidatorConfiguration.setEnableEntityMatchGA(enableEntityMatchGA);
            return this;
        }

        public Builder importFromS3(BusinessEntity businessEntity) {
            if (BusinessEntity.Product != businessEntity) {
                importFromS3.setSkipStep(true);
            }
            return this;
        }

    }
}
