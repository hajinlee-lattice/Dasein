package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.CommitEntityMatchWorkflowConfiguration;

public class ProcessAccountWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private ProcessAccountWorkflowConfiguration configuration = new ProcessAccountWorkflowConfiguration();
        private ProcessAccountStepConfiguration processAccountStepConfiguration = new ProcessAccountStepConfiguration();
        private CommitEntityMatchWorkflowConfiguration.Builder commitMatch = new CommitEntityMatchWorkflowConfiguration.Builder();
        private UpdateAccountWorkflowConfiguration.Builder updateAccountWorkflowBuilder = new UpdateAccountWorkflowConfiguration.Builder();
        private RebuildAccountWorkflowConfiguration.Builder rebuildAccountWorkflowBuilder = new RebuildAccountWorkflowConfiguration.Builder();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            commitMatch.customer(customerSpace);
            processAccountStepConfiguration.setCustomerSpace(customerSpace);
            updateAccountWorkflowBuilder.customer(customerSpace);
            rebuildAccountWorkflowBuilder.customer(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            processAccountStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            updateAccountWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            rebuildAccountWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder dataCloudVersion(DataCloudVersion dataCloudVersion) {
            processAccountStepConfiguration.setDataCloudVersion(dataCloudVersion.getVersion());
            updateAccountWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            rebuildAccountWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder entityMatchEnabled(boolean entityMatchEnabled) {
            processAccountStepConfiguration.setEntityMatchEnabled(entityMatchEnabled);
            rebuildAccountWorkflowBuilder.entityMatchEnabled(entityMatchEnabled);
            return this;
        }

        public Builder rebuildEntities(Set<BusinessEntity> entities) {
            if (CollectionUtils.isNotEmpty(entities)) {
                if (entities.contains(BusinessEntity.Account)) {
                    processAccountStepConfiguration.setRebuild(true);
                    updateAccountWorkflowBuilder.rebuildEntities(entities);
                    rebuildAccountWorkflowBuilder.rebuildEntities(entities);
                }
            }
            return this;
        }

        public Builder skipEntitties(Set<BusinessEntity> entities) {
            if (CollectionUtils.isNotEmpty(entities)) {
                if (entities.contains(BusinessEntity.Account)) {
                    rebuildAccountWorkflowBuilder.build().setSkipCompletedSteps(true);
                }
            }
            return this;
        }

        public Builder dataQuotaLimit(Long dataQuotaLimit) {
            processAccountStepConfiguration.setDataQuotaLimit(dataQuotaLimit);
            updateAccountWorkflowBuilder.dataQuotaLimit(dataQuotaLimit);
            rebuildAccountWorkflowBuilder.dataQuotaLimit(dataQuotaLimit);
            return this;
        }

        public ProcessAccountWorkflowConfiguration build() {
            configuration.setContainerConfiguration("processAccountWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            configuration.add(processAccountStepConfiguration);
            configuration.add(commitMatch.build());
            configuration.add(updateAccountWorkflowBuilder.build());
            configuration.add(rebuildAccountWorkflowBuilder.build());
            return configuration;
        }
    }
}
