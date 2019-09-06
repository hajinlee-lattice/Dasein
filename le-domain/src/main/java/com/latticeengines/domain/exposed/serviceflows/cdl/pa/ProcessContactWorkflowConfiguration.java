package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;

public class ProcessContactWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {

        private ProcessContactWorkflowConfiguration configuration = new ProcessContactWorkflowConfiguration();
        private ProcessContactStepConfiguration processContactStepConfiguration = new ProcessContactStepConfiguration();
        private UpdateContactWorkflowConfiguration.Builder updateContactWorkflowBuilder = new UpdateContactWorkflowConfiguration.Builder();
        private RebuildContactWorkflowConfiguration.Builder rebuildContactWorkflowBuilder = new RebuildContactWorkflowConfiguration.Builder();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            processContactStepConfiguration.setCustomerSpace(customerSpace);
            updateContactWorkflowBuilder.customer(customerSpace);
            rebuildContactWorkflowBuilder.customer(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            processContactStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            updateContactWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            rebuildContactWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public Builder workflowContainerMem(int mb) {
            configuration.setContainerMemoryMB(mb);
            return this;
        }

        public Builder userId(String userId) {
            configuration.setUserId(userId);
            return this;
        }

        public Builder entityMatchEnabled(boolean entityMatchEnabled) {
            processContactStepConfiguration.setEntityMatchEnabled(entityMatchEnabled);
            rebuildContactWorkflowBuilder.entityMatchEnabled(entityMatchEnabled);
            return this;
        }

        public Builder rebuildEntities(Set<BusinessEntity> entities) {
            if (CollectionUtils.isNotEmpty(entities)) {
                if (entities.contains(BusinessEntity.Contact)) {
                    processContactStepConfiguration.setRebuild(true);
                    updateContactWorkflowBuilder.rebuildEntities(entities);
                    rebuildContactWorkflowBuilder.rebuildEntities(entities);
                }
            }
            return this;
        }

        public  Builder setCleanup(Boolean needCleanup) {
                processContactStepConfiguration.setNeedCleanup(needCleanup);
                updateContactWorkflowBuilder.setCleanup(needCleanup);
                rebuildContactWorkflowBuilder.setCleanup(needCleanup);
            return this;
        }

        public ProcessContactWorkflowConfiguration build() {
            configuration.setContainerConfiguration("processContactWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            configuration.add(processContactStepConfiguration);
            configuration.add(updateContactWorkflowBuilder.build());
            configuration.add(rebuildContactWorkflowBuilder.build());
            return configuration;
        }
    }
}
