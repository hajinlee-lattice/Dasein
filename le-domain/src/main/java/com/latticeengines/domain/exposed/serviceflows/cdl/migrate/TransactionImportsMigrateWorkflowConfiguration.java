package com.latticeengines.domain.exposed.serviceflows.cdl.migrate;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.MigrateImportServiceConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.TransactionTemplateMigrateStepConfiguration;

public class TransactionImportsMigrateWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public TransactionImportsMigrateWorkflowConfiguration() {
    }

    public static class Builder {
        private TransactionImportsMigrateWorkflowConfiguration configuration = new TransactionImportsMigrateWorkflowConfiguration();

        private TransactionTemplateMigrateStepConfiguration importTemplateMigrateStepConfiguration = new TransactionTemplateMigrateStepConfiguration();
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
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            importTemplateMigrateStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            convertBatchStoreConfigurationBuilder.microServiceHostPort(microServiceHostPort);
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
            migrateImportServiceConfiguration.setEntity(BusinessEntity.Transaction);
            convertBatchStoreConfigurationBuilder.convertBatchStoreServiceConfiguration(migrateImportServiceConfiguration);
            return this;
        }

        public TransactionImportsMigrateWorkflowConfiguration build() {
            configuration.setContainerConfiguration("transactionImportsMigrateWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            convertBatchStoreConfigurationBuilder.entity(BusinessEntity.Transaction);
            configuration.add(importTemplateMigrateStepConfiguration);
            configuration.add(convertBatchStoreConfigurationBuilder.build());
            return configuration;
        }
    }
}
