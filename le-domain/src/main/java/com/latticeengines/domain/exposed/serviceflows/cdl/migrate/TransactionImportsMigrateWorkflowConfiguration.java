package com.latticeengines.domain.exposed.serviceflows.cdl.migrate;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.ImportTemplateMigrateStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.MigrateTransactionImportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.RegisterImportActionStepConfiguration;

public class TransactionImportsMigrateWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public TransactionImportsMigrateWorkflowConfiguration() {
    }

    public static class Builder {
        private TransactionImportsMigrateWorkflowConfiguration configuration = new TransactionImportsMigrateWorkflowConfiguration();

        private ImportTemplateMigrateStepConfiguration importTemplateMigrateStepConfiguration = new ImportTemplateMigrateStepConfiguration();
        private MigrateTransactionImportStepConfiguration migrateTransactionImportStepConfiguration = new MigrateTransactionImportStepConfiguration();
        private RegisterImportActionStepConfiguration registerImportActionStepConfiguration = new RegisterImportActionStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            importTemplateMigrateStepConfiguration.setCustomerSpace(customerSpace);
            migrateTransactionImportStepConfiguration.setCustomerSpace(customerSpace);
            registerImportActionStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            importTemplateMigrateStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            migrateTransactionImportStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            registerImportActionStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            importTemplateMigrateStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            registerImportActionStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder userId(String userId) {
            configuration.setUserId(userId);
            return this;
        }

        public Builder dataFeedTaskList(List<String> dataFeedTaskList) {
            if (CollectionUtils.isEmpty(dataFeedTaskList)) {
                importTemplateMigrateStepConfiguration.setSkipStep(true);
                migrateTransactionImportStepConfiguration.setSkipStep(true);
                registerImportActionStepConfiguration.setSkipStep(true);
            } else {
                importTemplateMigrateStepConfiguration.setDataFeedTaskList(dataFeedTaskList);
                migrateTransactionImportStepConfiguration.setDataFeedTaskList(dataFeedTaskList);
            }
            return this;
        }

        public TransactionImportsMigrateWorkflowConfiguration build() {
            configuration.setContainerConfiguration("transactionImportsMigrationWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            configuration.add(importTemplateMigrateStepConfiguration);
            configuration.add(migrateTransactionImportStepConfiguration);
            configuration.add(registerImportActionStepConfiguration);
            return configuration;
        }
    }
}
