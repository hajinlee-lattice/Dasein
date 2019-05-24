package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;

public class MatchEntityWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private MatchEntityWorkflowConfiguration configuration = new MatchEntityWorkflowConfiguration();

        private ProcessAccountStepConfiguration processAccountStepConfiguration = new ProcessAccountStepConfiguration();
        private ProcessContactStepConfiguration processContactStepConfiguration = new ProcessContactStepConfiguration();
        private ProcessStepConfiguration processStepConfiguration = new ProcessStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            processAccountStepConfiguration.setCustomerSpace(customerSpace);
            processContactStepConfiguration.setCustomerSpace(customerSpace);
            processStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            processAccountStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            processContactStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            processStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder dataCloudVersion(DataCloudVersion dataCloudVersion) {
            processAccountStepConfiguration.setDataCloudVersion(dataCloudVersion.getVersion());
            return this;
        }

        public Builder entityMatchEnabled(boolean entityMatchEnabled) {
            processAccountStepConfiguration.setEntityMatchEnabled(entityMatchEnabled);
            processContactStepConfiguration.setEntityMatchEnabled(entityMatchEnabled);
            processStepConfiguration.setEntityMatchEnabled(entityMatchEnabled);
            return this;
        }

        public MatchEntityWorkflowConfiguration build() {
            configuration.setContainerConfiguration("matchEntityWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(processAccountStepConfiguration);
            configuration.add(processContactStepConfiguration);
            configuration.add(processStepConfiguration);
            return configuration;
        }
    }
}
