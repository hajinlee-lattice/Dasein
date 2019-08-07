package com.latticeengines.domain.exposed.serviceflows.cdl.migrate;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.AccountTemplateMigrateStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.MigrateImportServiceConfiguration;

public class AccountImportsMigrateWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public AccountImportsMigrateWorkflowConfiguration() {
    }

    public static class Builder {
        private AccountImportsMigrateWorkflowConfiguration configuration = new AccountImportsMigrateWorkflowConfiguration();

        private AccountTemplateMigrateStepConfiguration importTemplateMigrateStepConfiguration = new AccountTemplateMigrateStepConfiguration();
        private ConvertBatchStoreToImportWorkflowConfiguration.Builder convertBatchStoreConfigurationBuilder =
                new ConvertBatchStoreToImportWorkflowConfiguration.Builder();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            importTemplateMigrateStepConfiguration.setCustomerSpace(customerSpace);
            convertBatchStoreConfigurationBuilder.customer(customerSpace);
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
            } else {
                importTemplateMigrateStepConfiguration.setDataFeedTaskList(dataFeedTaskList);
            }
            return this;
        }

        public Builder action(Action action) {
            if (action == null) {
                importTemplateMigrateStepConfiguration.setSkipStep(true);
                convertBatchStoreConfigurationBuilder.skipConvert(true);
            } else {
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
            return this;
        }

        public AccountImportsMigrateWorkflowConfiguration build() {
            configuration.setContainerConfiguration("accountImportsMigrateWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            convertBatchStoreConfigurationBuilder.entity(BusinessEntity.Account);
            configuration.add(importTemplateMigrateStepConfiguration);
            configuration.add(convertBatchStoreConfigurationBuilder.build());
            return configuration;
        }
    }

}
