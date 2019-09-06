package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessProductStepConfiguration;

public class ProcessProductWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private ProcessProductWorkflowConfiguration configuration = new ProcessProductWorkflowConfiguration();
        private ProcessProductStepConfiguration processProductStepConfiguration = new ProcessProductStepConfiguration();
        private UpdateProductWorkflowConfiguration.Builder updateProductWorkflowBuilder = new UpdateProductWorkflowConfiguration.Builder();
        private RebuildProductWorkflowConfiguration.Builder rebuildProductWorkflowBuilder = new RebuildProductWorkflowConfiguration.Builder();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            processProductStepConfiguration.setCustomerSpace(customerSpace);
            updateProductWorkflowBuilder.customer(customerSpace);
            rebuildProductWorkflowBuilder.customer(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            processProductStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            updateProductWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            rebuildProductWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder rebuildEntities(Set<BusinessEntity> entities) {
            if (CollectionUtils.isNotEmpty(entities)) {
                if (entities.contains(BusinessEntity.Product)) {
                    processProductStepConfiguration.setRebuild(true);
                    updateProductWorkflowBuilder.rebuildEntities(entities);
                    rebuildProductWorkflowBuilder.rebuildEntities(entities);
                }
            }
            return this;
        }

        public Builder setCleanup(Boolean needCleanup) {
            processProductStepConfiguration.setNeedCleanup(needCleanup);
            updateProductWorkflowBuilder.setCleanup(needCleanup);
            rebuildProductWorkflowBuilder.setCleanup(needCleanup);
            return this;
        }

        public ProcessProductWorkflowConfiguration build() {
            configuration.setContainerConfiguration("processProductWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            configuration.add(processProductStepConfiguration);
            configuration.add(updateProductWorkflowBuilder.build());
            configuration.add(rebuildProductWorkflowBuilder.build());
            return configuration;
        }
    }
}
