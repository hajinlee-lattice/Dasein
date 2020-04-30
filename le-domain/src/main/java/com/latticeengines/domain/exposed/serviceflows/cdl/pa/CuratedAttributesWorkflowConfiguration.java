package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.CuratedAccountAttributesStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.CuratedContactAttributesStepConfiguration;

public class CuratedAttributesWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private CuratedAttributesWorkflowConfiguration configuration = new CuratedAttributesWorkflowConfiguration();
        private CuratedAccountAttributesStepConfiguration curatedAccountAttributesStepConfiguration = new CuratedAccountAttributesStepConfiguration();
        private CuratedContactAttributesStepConfiguration curatedContactAttributesStepConfiguration= new CuratedContactAttributesStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            curatedAccountAttributesStepConfiguration.setCustomerSpace(customerSpace);
            curatedContactAttributesStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            curatedAccountAttributesStepConfiguration
                    .setInternalResourceHostPort(internalResourceHostPort);
            curatedContactAttributesStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
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

        // CuratedAccountAttributeStep should be run during rebuild if the CuratedAccount BusinessEntity is included
        // in the set of entities to rebuild, even if there is neither an new Account nor Contact CSV being imported.
        public Builder rebuildEntities(Set<BusinessEntity> entities) {
            if (CollectionUtils.isNotEmpty(entities)) {
                if (entities.contains(BusinessEntity.CuratedAccount)) {
                    curatedAccountAttributesStepConfiguration.setRebuild(true);
                }
                if(entities.contains(BusinessEntity.CuratedContact)){
                    curatedContactAttributesStepConfiguration.setRebuild(true);
                }
            }
            return this;
        }

        public CuratedAttributesWorkflowConfiguration build() {
            configuration.setContainerConfiguration("curatedAttributesWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            configuration.add(curatedAccountAttributesStepConfiguration);
            configuration.add(curatedContactAttributesStepConfiguration);
            return configuration;
        }


    }
}
