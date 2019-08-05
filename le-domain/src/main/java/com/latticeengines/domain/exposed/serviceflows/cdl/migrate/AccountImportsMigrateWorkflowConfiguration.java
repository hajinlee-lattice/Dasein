package com.latticeengines.domain.exposed.serviceflows.cdl.migrate;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.ImportTemplateMigrateStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.MigrateImportServiceConfiguration;

public class AccountImportsMigrateWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public AccountImportsMigrateWorkflowConfiguration() {
    }

    public static class Builder {
        private AccountImportsMigrateWorkflowConfiguration configuration = new AccountImportsMigrateWorkflowConfiguration();

        private ImportTemplateMigrateStepConfiguration importTemplateMigrateStepConfiguration = new ImportTemplateMigrateStepConfiguration();
//        private MigrateAccountImportStepConfiguration migrateAccountImportStepConfiguration = new MigrateAccountImportStepConfiguration();
//        private RegisterImportActionStepConfiguration registerImportActionStepConfiguration = new RegisterImportActionStepConfiguration();
        private ConvertBatchStoreToImportWorkflowConfiguration.Builder convertBatchStoreConfigurationBuilder =
                new ConvertBatchStoreToImportWorkflowConfiguration.Builder();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            importTemplateMigrateStepConfiguration.setCustomerSpace(customerSpace);
            convertBatchStoreConfigurationBuilder.customer(customerSpace);

//            migrateAccountImportStepConfiguration.setCustomerSpace(customerSpace);
//            registerImportActionStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            importTemplateMigrateStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            convertBatchStoreConfigurationBuilder.internalResourceHostPort(internalResourceHostPort);
//            migrateAccountImportStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
//            registerImportActionStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            importTemplateMigrateStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            convertBatchStoreConfigurationBuilder.microServiceHostPort(microServiceHostPort);
//            registerImportActionStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder userId(String userId) {
            configuration.setUserId(userId);
            convertBatchStoreConfigurationBuilder.userId(userId);
            return this;
        }

        public Builder dataFeedTaskList(List<String> dataFeedTaskList) {
            if (CollectionUtils.isEmpty(dataFeedTaskList)) {
                importTemplateMigrateStepConfiguration.setSkipStep(true);
                convertBatchStoreConfigurationBuilder.skipConvert(true);
//                migrateAccountImportStepConfiguration.setSkipStep(true);
//                registerImportActionStepConfiguration.setSkipStep(true);
            } else {
                importTemplateMigrateStepConfiguration.setDataFeedTaskList(dataFeedTaskList);
//                migrateAccountImportStepConfiguration.setDataFeedTaskList(dataFeedTaskList);
            }
            return this;
        }

        public Builder action(Action action) {
            if (action == null) {
                importTemplateMigrateStepConfiguration.setSkipStep(true);
                convertBatchStoreConfigurationBuilder.skipConvert(true);
//                migrateAccountImportStepConfiguration.setSkipStep(true);
//                registerImportActionStepConfiguration.setSkipStep(true);
            } else {
//                registerImportActionStepConfiguration.setActionPid(action.getPid());
                convertBatchStoreConfigurationBuilder.actionPid(action.getPid());
            }
            return this;
        }

        public Builder migrateTracking(Long trackingPid) {
            importTemplateMigrateStepConfiguration.setMigrateTrackingPid(trackingPid);
            MigrateImportServiceConfiguration migrateImportServiceConfiguration = new MigrateImportServiceConfiguration();
            migrateImportServiceConfiguration.setMigrateTrackingPid(trackingPid);
            migrateImportServiceConfiguration.setEntity(BusinessEntity.Account);
            convertBatchStoreConfigurationBuilder.convertBatchStoreServiceConfiguration(migrateImportServiceConfiguration);
//            migrateAccountImportStepConfiguration.setMigrateTrackingPid(trackingPid);
//            registerImportActionStepConfiguration.setMigrateTrackingPid(trackingPid);
            return this;
        }

        public AccountImportsMigrateWorkflowConfiguration build() {
            configuration.setContainerConfiguration("accountImportsMigrationWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            convertBatchStoreConfigurationBuilder.entity(BusinessEntity.Account);
//            registerImportActionStepConfiguration.setEntity(BusinessEntity.Account);
            configuration.add(importTemplateMigrateStepConfiguration);
            configuration.add(convertBatchStoreConfigurationBuilder.build());
//            configuration.add(migrateAccountImportStepConfiguration);
//            configuration.add(registerImportActionStepConfiguration);
            return configuration;
        }
    }

}
