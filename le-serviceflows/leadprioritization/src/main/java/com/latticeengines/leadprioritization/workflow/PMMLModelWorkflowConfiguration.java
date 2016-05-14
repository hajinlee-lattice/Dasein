package com.latticeengines.leadprioritization.workflow;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.CreatePMMLModelConfiguration;

public class PMMLModelWorkflowConfiguration extends WorkflowConfiguration {

    public static class Builder {
        private PMMLModelWorkflowConfiguration configuration = new PMMLModelWorkflowConfiguration();
        private CreatePMMLModelConfiguration model = new CreatePMMLModelConfiguration();

        public Builder podId(String podId) {
            model.setPodId(podId);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            model.setMicroServiceHostPort(microServiceHostPort);
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

        public Builder module(String moduleName) {
            model.setModuleName(moduleName);
            return this;
        }

        public Builder pmmlArtifactPath(String pmmlArtifactPath) {
            model.setPmmlArtifactPath(pmmlArtifactPath);
            return this;
        }

        public Builder pivotArtifactPath(String pivotArtifactPath) {
            model.setPivotArtifactPath(pivotArtifactPath);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }
        
        public Builder internalResourceHostPort(String internalResourceHostPort) {
            model.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public PMMLModelWorkflowConfiguration build() {
            configuration.add(model);
            return configuration;
        }

    }
}
