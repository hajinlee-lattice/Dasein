package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.AWSPythonBatchConfiguration;

public class RebuildTransactionWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {

        private RebuildTransactionWorkflowConfiguration configuration = new RebuildTransactionWorkflowConfiguration();
        private ProcessTransactionStepConfiguration processTransactionStepConfiguration = new ProcessTransactionStepConfiguration();
        private AWSPythonBatchConfiguration awsPythonDataConfiguration = new AWSPythonBatchConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            processTransactionStepConfiguration.setCustomerSpace(customerSpace);
            awsPythonDataConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            processTransactionStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            awsPythonDataConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            awsPythonDataConfiguration.setMicroServiceHostPort(microServiceHostPort);
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
            awsPythonDataConfiguration.setRollingPeriod(apsRollingPeriod);
            return this;
        }

        public RebuildTransactionWorkflowConfiguration build() {
            configuration.setContainerConfiguration("rebuildTransactionWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(processTransactionStepConfiguration);
            awsPythonDataConfiguration.setSubmission(true);
            configuration.add(awsPythonDataConfiguration);
            return configuration;
        }
    }
}
