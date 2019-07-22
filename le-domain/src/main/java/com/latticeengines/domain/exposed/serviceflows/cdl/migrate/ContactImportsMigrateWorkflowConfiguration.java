package com.latticeengines.domain.exposed.serviceflows.cdl.migrate;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.ImportTemplateMigrateStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.MigrateContactImportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.RegisterImportActionStepConfiguration;

public class ContactImportsMigrateWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public ContactImportsMigrateWorkflowConfiguration() {
    }

    public static class Builder {
        private ContactImportsMigrateWorkflowConfiguration configuration =
                new ContactImportsMigrateWorkflowConfiguration();

        private ImportTemplateMigrateStepConfiguration importTemplateMigrateStepConfiguration = new ImportTemplateMigrateStepConfiguration();
        private MigrateContactImportStepConfiguration migrateContactImportStepConfiguration = new MigrateContactImportStepConfiguration();
        private RegisterImportActionStepConfiguration registerImportActionStepConfiguration = new RegisterImportActionStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            importTemplateMigrateStepConfiguration.setCustomerSpace(customerSpace);
            migrateContactImportStepConfiguration.setCustomerSpace(customerSpace);
            registerImportActionStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            importTemplateMigrateStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            migrateContactImportStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
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
                migrateContactImportStepConfiguration.setSkipStep(true);
                registerImportActionStepConfiguration.setSkipStep(true);
            } else {
                importTemplateMigrateStepConfiguration.setDataFeedTaskList(dataFeedTaskList);
                migrateContactImportStepConfiguration.setDataFeedTaskList(dataFeedTaskList);
            }
            return this;
        }

        public ContactImportsMigrateWorkflowConfiguration build() {
            configuration.setContainerConfiguration("contactImportsMigrationWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(importTemplateMigrateStepConfiguration);
            configuration.add(migrateContactImportStepConfiguration);
            configuration.add(registerImportActionStepConfiguration);
            return configuration;
        }
    }
}
