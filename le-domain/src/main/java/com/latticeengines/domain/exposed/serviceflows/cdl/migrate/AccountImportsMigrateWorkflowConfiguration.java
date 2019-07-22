package com.latticeengines.domain.exposed.serviceflows.cdl.migrate;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.ImportTemplateMigrateStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.MigrateAccountImportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.RegisterImportActionStepConfiguration;

public class AccountImportsMigrateWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public AccountImportsMigrateWorkflowConfiguration() {
    }

    public static class Builder {
        private AccountImportsMigrateWorkflowConfiguration configuration = new AccountImportsMigrateWorkflowConfiguration();

        private ImportTemplateMigrateStepConfiguration importTemplateMigrateStepConfiguration = new ImportTemplateMigrateStepConfiguration();
        private MigrateAccountImportStepConfiguration migrateAccountImportStepConfiguration = new MigrateAccountImportStepConfiguration();
        private RegisterImportActionStepConfiguration registerImportActionStepConfiguration = new RegisterImportActionStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            importTemplateMigrateStepConfiguration.setCustomerSpace(customerSpace);
            migrateAccountImportStepConfiguration.setCustomerSpace(customerSpace);
            registerImportActionStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            importTemplateMigrateStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            migrateAccountImportStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
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
                migrateAccountImportStepConfiguration.setSkipStep(true);
                registerImportActionStepConfiguration.setSkipStep(true);
            } else {
                importTemplateMigrateStepConfiguration.setDataFeedTaskList(dataFeedTaskList);
                migrateAccountImportStepConfiguration.setDataFeedTaskList(dataFeedTaskList);
            }
            return this;
        }

        public AccountImportsMigrateWorkflowConfiguration build() {
            configuration.setContainerConfiguration("accountImportsMigrationWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(importTemplateMigrateStepConfiguration);
            configuration.add(migrateAccountImportStepConfiguration);
            configuration.add(registerImportActionStepConfiguration);
            return configuration;
        }
    }

}
