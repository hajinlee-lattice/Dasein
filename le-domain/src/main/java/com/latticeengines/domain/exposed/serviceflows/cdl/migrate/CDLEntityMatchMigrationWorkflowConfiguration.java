package com.latticeengines.domain.exposed.serviceflows.cdl.migrate;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.EntityMatchMigrateStepConfiguration;

public class CDLEntityMatchMigrationWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public CDLEntityMatchMigrationWorkflowConfiguration(){
    }

    public static class Builder {
        private CDLEntityMatchMigrationWorkflowConfiguration  configuration =
                new CDLEntityMatchMigrationWorkflowConfiguration();
        private EntityMatchMigrateStepConfiguration startMigrateConfiguration =
                new EntityMatchMigrateStepConfiguration();
        private EntityMatchMigrateStepConfiguration finishMigrateConfiguration =
                new EntityMatchMigrateStepConfiguration();
        private AccountImportsMigrateWorkflowConfiguration.Builder accountImportMigrateConfigurationBuilder =
                new AccountImportsMigrateWorkflowConfiguration.Builder();
        private ContactImportsMigrateWorkflowConfiguration.Builder contactImportMigrateConfigurationBuilder =
                new ContactImportsMigrateWorkflowConfiguration.Builder();
        private TransactionImportsMigrateWorkflowConfiguration.Builder transactionImportMigrateConfigurationBuilder =
                new TransactionImportsMigrateWorkflowConfiguration.Builder();


        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            startMigrateConfiguration.setCustomerSpace(customerSpace);
            finishMigrateConfiguration.setCustomerSpace(customerSpace);
            accountImportMigrateConfigurationBuilder.customer(customerSpace);
            contactImportMigrateConfigurationBuilder.customer(customerSpace);
            transactionImportMigrateConfigurationBuilder.customer(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            startMigrateConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            finishMigrateConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            accountImportMigrateConfigurationBuilder.internalResourceHostPort(internalResourceHostPort);
            contactImportMigrateConfigurationBuilder.internalResourceHostPort(internalResourceHostPort);
            transactionImportMigrateConfigurationBuilder.internalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            startMigrateConfiguration.setMicroServiceHostPort(microServiceHostPort);
            finishMigrateConfiguration.setMicroServiceHostPort(microServiceHostPort);
            accountImportMigrateConfigurationBuilder.microServiceHostPort(microServiceHostPort);
            contactImportMigrateConfigurationBuilder.microServiceHostPort(microServiceHostPort);
            transactionImportMigrateConfigurationBuilder.microServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder userId(String userId) {
            configuration.setUserId(userId);
            accountImportMigrateConfigurationBuilder.userId(userId);
            contactImportMigrateConfigurationBuilder.userId(userId);
            transactionImportMigrateConfigurationBuilder.userId(userId);
            return this;
        }

        public Builder dataFeedTaskMap(Map<BusinessEntity, List<String>> dataFeedTaskMap) {
            if (MapUtils.isEmpty(dataFeedTaskMap)) {
                throw new RuntimeException("There's no template to be migrated!");
            }
            accountImportMigrateConfigurationBuilder.dataFeedTaskList(dataFeedTaskMap.get(BusinessEntity.Account));
            contactImportMigrateConfigurationBuilder.dataFeedTaskList(dataFeedTaskMap.get(BusinessEntity.Contact));
            transactionImportMigrateConfigurationBuilder.dataFeedTaskList(dataFeedTaskMap.get(BusinessEntity.Transaction));
            return this;
        }

        public CDLEntityMatchMigrationWorkflowConfiguration build() {
            configuration.setContainerConfiguration("cdlEntityMatchMigrationWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(startMigrateConfiguration);
            configuration.add(accountImportMigrateConfigurationBuilder.build());
            configuration.add(contactImportMigrateConfigurationBuilder.build());
            configuration.add(transactionImportMigrateConfigurationBuilder.build());
            configuration.add(finishMigrateConfiguration);
            return configuration;
        }

    }

}
