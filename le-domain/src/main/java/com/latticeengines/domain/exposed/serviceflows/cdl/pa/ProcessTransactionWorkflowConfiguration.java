package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;

public class ProcessTransactionWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private ProcessTransactionWorkflowConfiguration configuration = new ProcessTransactionWorkflowConfiguration();
        private ProcessTransactionStepConfiguration processTransactionStepConfiguration = new ProcessTransactionStepConfiguration();
        private UpdateTransactionWorkflowConfiguration.Builder updateTransactionWorkflowBuilder = new UpdateTransactionWorkflowConfiguration.Builder();
        private RebuildTransactionWorkflowConfiguration.Builder rebuildTransactionWorkflowBuilder = new RebuildTransactionWorkflowConfiguration.Builder();

        public Builder actionIds(List<Long> actionIds) {
            processTransactionStepConfiguration.setActionIds(actionIds);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            processTransactionStepConfiguration.setCustomerSpace(customerSpace);
            updateTransactionWorkflowBuilder.customer(customerSpace);
            rebuildTransactionWorkflowBuilder.customer(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            processTransactionStepConfiguration
                    .setInternalResourceHostPort(internalResourceHostPort);
            updateTransactionWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            rebuildTransactionWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            updateTransactionWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            rebuildTransactionWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder rebuildEntities(Set<BusinessEntity> entities) {
            if (CollectionUtils.isNotEmpty(entities)) {
                if (entities.contains(BusinessEntity.Transaction)) {
                    processTransactionStepConfiguration.setRebuild(true);
                    updateTransactionWorkflowBuilder.rebuildEntities(entities);
                    rebuildTransactionWorkflowBuilder.rebuildEntities(entities);
                }
            }
            return this;
        }

        public Builder setReplace(boolean needReplace) {
            processTransactionStepConfiguration.setNeedReplace(needReplace);
            updateTransactionWorkflowBuilder.setReplace(needReplace);
            rebuildTransactionWorkflowBuilder.setReplace(needReplace);
            return this;
        }

        public Builder entityMatchEnabled(boolean entityMatchEnabled) {
            processTransactionStepConfiguration.setEntityMatchEnabled(entityMatchEnabled);
            rebuildTransactionWorkflowBuilder.entityMatchEnabled(entityMatchEnabled);
            updateTransactionWorkflowBuilder.entityMatchEnabled(entityMatchEnabled);
            return this;
        }

        public ProcessTransactionWorkflowConfiguration build() {
            configuration.setContainerConfiguration("processTransactionWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            configuration.add(processTransactionStepConfiguration);
            configuration.add(updateTransactionWorkflowBuilder.build());
            configuration.add(rebuildTransactionWorkflowBuilder.build());
            return configuration;
        }
    }
}
