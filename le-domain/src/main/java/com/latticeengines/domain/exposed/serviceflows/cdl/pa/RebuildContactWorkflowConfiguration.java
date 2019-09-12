package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;

public class RebuildContactWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {

        private RebuildContactWorkflowConfiguration configuration = new RebuildContactWorkflowConfiguration();
        private ProcessContactStepConfiguration processContactStepConfiguration = new ProcessContactStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            processContactStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            processContactStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
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

        public Builder rebuildEntities(Set<BusinessEntity> entities) {
            if (CollectionUtils.isNotEmpty(entities)) {
                if (entities.contains(BusinessEntity.Contact)) {
                    processContactStepConfiguration.setRebuild(true);
                }
            }
            return this;
        }

        public Builder setReplace(boolean needReplace) {
            processContactStepConfiguration.setNeedReplace(needReplace);
            return this;
        }

        public Builder entityMatchEnabled(boolean entityMatchEnabled) {
            processContactStepConfiguration.setEntityMatchEnabled(entityMatchEnabled);
            return this;
        }

        public RebuildContactWorkflowConfiguration build() {
            configuration.setContainerConfiguration("rebuildContactWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            configuration.add(processContactStepConfiguration);
            return configuration;
        }
    }
}
