package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ImportDataFeedTaskConfiguration;
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

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("cdlDataFeedImportWorkflow", customerSpace,
                    configuration.getClass().getSimpleName());
            importDataFeedTaskConfiguration.setCustomerSpace(customerSpace);
            exportToS3.setCustomerSpace(customerSpace);
            inputFileValidatorConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            importDataFeedTaskConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            exportToS3.setInternalResourceHostPort(internalResourceHostPort);
            inputFileValidatorConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder dataFeedTaskId(String dataFeedTaskId) {
            importDataFeedTaskConfiguration.setDataFeedTaskId(dataFeedTaskId);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            importDataFeedTaskConfiguration.setMicroServiceHostPort(microServiceHostPort);
            exportToS3.setMicroServiceHostPort(microServiceHostPort);
            inputFileValidatorConfiguration.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder importConfig(String importConfig) {
            importDataFeedTaskConfiguration.setImportConfig(importConfig);
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
            configuration.add(importDataFeedTaskConfiguration);
            configuration.add(exportToS3);
            configuration.add(inputFileValidatorConfiguration);
            return configuration;
        }

        // file with entity Account, Contact, Product
        public Builder fileValidation(BusinessEntity businessEntity) {
            if (BusinessEntity.Account != businessEntity && BusinessEntity.Contact != businessEntity
                    && BusinessEntity.Product != businessEntity) {
                inputFileValidatorConfiguration.setSkipStep(true);
            } else {
                inputFileValidatorConfiguration.setEntity(businessEntity);
            }
            return this;
        }

    }
}
