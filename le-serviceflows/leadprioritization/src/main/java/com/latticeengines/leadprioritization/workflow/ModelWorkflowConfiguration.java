package com.latticeengines.leadprioritization.workflow;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.modeling.ModelStepConfiguration;

public class ModelWorkflowConfiguration extends WorkflowConfiguration {
    public static class Builder {
        private ModelWorkflowConfiguration configuration = new ModelWorkflowConfiguration();
        private ModelStepConfiguration model = new ModelStepConfiguration();

        public Builder microServiceHostPort(String microServiceHostPort) {
            model.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            model.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            model.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder workflow(String workflowName) {
            configuration.setWorkflowName(workflowName);
            configuration.setName(workflowName);
            return this;
        }

        public Builder modelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
            model.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
            return this;
        }

        public Builder modelName(String modelName) {
            model.setModelName(modelName);
            return this;
        }

        public Builder eventTableName(String eventTableName) {
            model.setEventTableName(eventTableName);
            return this;
        }

        public ModelWorkflowConfiguration build() {
            configuration.add(model);

            return configuration;
        }
    }
}
