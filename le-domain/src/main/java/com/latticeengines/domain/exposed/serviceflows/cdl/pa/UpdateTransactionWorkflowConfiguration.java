package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;

public class UpdateTransactionWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {

        private UpdateTransactionWorkflowConfiguration configuration = new UpdateTransactionWorkflowConfiguration();
        private ProcessTransactionStepConfiguration processTransactionStepConfiguration = new ProcessTransactionStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            processTransactionStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            processTransactionStepConfiguration
                    .setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
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
                if (entities.contains(BusinessEntity.Transaction)) {
                    processTransactionStepConfiguration.setRebuild(true);
                }
            }
            return this;
        }

        public Builder apsRollingPeriod(String apsRollingPeriod) {
            return this;
        }

        public Builder entityMatchEnabled(boolean entityMatchEnabled) {
            processTransactionStepConfiguration.setEntityMatchEnabled(entityMatchEnabled);
            return this;
        }

        public UpdateTransactionWorkflowConfiguration build() {
            configuration.setContainerConfiguration("updateTransactionWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            configuration.add(processTransactionStepConfiguration);
            return configuration;
        }
    }
}
