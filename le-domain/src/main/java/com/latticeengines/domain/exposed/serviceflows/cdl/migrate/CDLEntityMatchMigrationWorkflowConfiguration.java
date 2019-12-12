package com.latticeengines.domain.exposed.serviceflows.cdl.migrate;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.EntityMatchMigrateStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

public class CDLEntityMatchMigrationWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public CDLEntityMatchMigrationWorkflowConfiguration(){
    }

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.DataCloud.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
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
        private ImportExportS3StepConfiguration exportToS3Configuration = new ImportExportS3StepConfiguration();


        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            startMigrateConfiguration.setCustomerSpace(customerSpace);
            finishMigrateConfiguration.setCustomerSpace(customerSpace);
            accountImportMigrateConfigurationBuilder.customer(customerSpace);
            contactImportMigrateConfigurationBuilder.customer(customerSpace);
            transactionImportMigrateConfigurationBuilder.customer(customerSpace);
            exportToS3Configuration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            startMigrateConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            finishMigrateConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            accountImportMigrateConfigurationBuilder.internalResourceHostPort(internalResourceHostPort);
            contactImportMigrateConfigurationBuilder.internalResourceHostPort(internalResourceHostPort);
            transactionImportMigrateConfigurationBuilder.internalResourceHostPort(internalResourceHostPort);
            exportToS3Configuration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            startMigrateConfiguration.setMicroServiceHostPort(microServiceHostPort);
            finishMigrateConfiguration.setMicroServiceHostPort(microServiceHostPort);
            accountImportMigrateConfigurationBuilder.microServiceHostPort(microServiceHostPort);
            contactImportMigrateConfigurationBuilder.microServiceHostPort(microServiceHostPort);
            transactionImportMigrateConfigurationBuilder.microServiceHostPort(microServiceHostPort);
            exportToS3Configuration.setMicroServiceHostPort(microServiceHostPort);
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
            startMigrateConfiguration.setDataFeedTaskMap(dataFeedTaskMap);
            accountImportMigrateConfigurationBuilder.dataFeedTaskList(dataFeedTaskMap.get(BusinessEntity.Account));
            contactImportMigrateConfigurationBuilder.dataFeedTaskList(dataFeedTaskMap.get(BusinessEntity.Contact));
            transactionImportMigrateConfigurationBuilder.dataFeedTaskList(dataFeedTaskMap.get(BusinessEntity.Transaction));
            finishMigrateConfiguration.setDataFeedTaskMap(dataFeedTaskMap);
            return this;
        }

        public Builder actionMap(Map<BusinessEntity, Action> actionMap) {
            if (MapUtils.isEmpty(actionMap)) {
                throw new RuntimeException("Migrate Action is empty!");
            }
            accountImportMigrateConfigurationBuilder.action(actionMap.get(BusinessEntity.Account));
            contactImportMigrateConfigurationBuilder.action(actionMap.get(BusinessEntity.Contact));
            transactionImportMigrateConfigurationBuilder.action(actionMap.get(BusinessEntity.Transaction));
            return this;
        }

        public Builder migrateTrackingPid(Long migrateTrackingPid) {
            if (migrateTrackingPid == null) {
                throw new RuntimeException("Migrate Tracking record id cannot be null!");
            }
            startMigrateConfiguration.setMigrateTrackingPid(migrateTrackingPid);
            accountImportMigrateConfigurationBuilder.migrateTracking(migrateTrackingPid);
            contactImportMigrateConfigurationBuilder.migrateTracking(migrateTrackingPid);
            transactionImportMigrateConfigurationBuilder.migrateTracking(migrateTrackingPid);
            finishMigrateConfiguration.setMigrateTrackingPid(migrateTrackingPid);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public CDLEntityMatchMigrationWorkflowConfiguration build() {
            configuration.setContainerConfiguration("cdlEntityMatchMigrationWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(startMigrateConfiguration);
            configuration.add(accountImportMigrateConfigurationBuilder.build());
            configuration.add(contactImportMigrateConfigurationBuilder.build());
            configuration.add(transactionImportMigrateConfigurationBuilder.build());
            configuration.add(exportToS3Configuration);
            configuration.add(finishMigrateConfiguration);
            return configuration;
        }

    }

}
