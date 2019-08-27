package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessProductStepConfiguration;

public class UpdateProductWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {

        private UpdateProductWorkflowConfiguration configuration = new UpdateProductWorkflowConfiguration();
        private ProcessProductStepConfiguration processProductStepConfiguration = new ProcessProductStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            processProductStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            processProductStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public Builder userId(String userId) {
            configuration.setUserId(userId);
            return this;
        }

        public Builder rebuildEntities(Set<BusinessEntity> entities) {
            if (CollectionUtils.isNotEmpty(entities)) {
                if (entities.contains(BusinessEntity.Product)) {
                    processProductStepConfiguration.setRebuild(true);
                }
            }
            return this;
        }

        public Builder cleanupEntities(Set<BusinessEntity> entities) {
            if (CollectionUtils.isNotEmpty(entities) && entities.contains(BusinessEntity.Product)) {
                processProductStepConfiguration.setNeedCleanup(true);
            }
            return this;
        }

        public UpdateProductWorkflowConfiguration build() {
            configuration.setContainerConfiguration("updateProductWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            configuration.add(processProductStepConfiguration);
            return configuration;
        }
    }
}
