package com.latticeengines.domain.exposed.serviceflows.modeling;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.CreatePMMLModelConfiguration;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.ModelStepConfiguration;

public class PMMLModelWorkflowConfiguration extends BaseModelingWorkflowConfiguration {

    public static class Builder {
        private PMMLModelWorkflowConfiguration configuration = new PMMLModelWorkflowConfiguration();
        private ModelStepConfiguration model = new ModelStepConfiguration();
        private CreatePMMLModelConfiguration pmml = new CreatePMMLModelConfiguration();
        private ImportExportS3StepConfiguration exportModelToS3 = new ImportExportS3StepConfiguration();

        public Builder podId(String podId) {
            pmml.setPodId(podId);
            model.setPodId(podId);
            exportModelToS3.setPodId(podId);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            pmml.setMicroServiceHostPort(microServiceHostPort);
            model.setMicroServiceHostPort(microServiceHostPort);
            exportModelToS3.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            model.setCustomerSpace(customerSpace);
            pmml.setCustomerSpace(customerSpace);
            exportModelToS3.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder modelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
            model.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
            pmml.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
            return this;
        }

        public Builder modelName(String modelName) {
            model.setModelName(modelName);
            pmml.setModelName(modelName);
            return this;
        }

        public Builder displayName(String displayName) {
            model.setDisplayName(displayName);
            pmml.setDisplayName(displayName);
            return this;
        }

        public Builder module(String moduleName) {
            model.setModuleName(moduleName);
            pmml.setModuleName(moduleName);
            return this;
        }

        public Builder pmmlArtifactPath(String pmmlArtifactPath) {
            pmml.setPmmlArtifactPath(pmmlArtifactPath);
            return this;
        }

        public Builder pivotArtifactPath(String pivotArtifactPath) {
            model.setPivotArtifactPath(pivotArtifactPath);
            pmml.setPivotArtifactPath(pivotArtifactPath);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            pmml.setInternalResourceHostPort(internalResourceHostPort);
            model.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            exportModelToS3.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder sourceSchemaInterpretation(String sourceSchemaInterpretation) {
            model.setSourceSchemaInterpretation(sourceSchemaInterpretation);
            pmml.setSourceSchemaInterpretation(sourceSchemaInterpretation);
            return this;
        }

        public Builder workflowContainerMem(int mb) {
            configuration.setContainerMemoryMB(mb);
            return this;
        }

        public Builder moduleName(String moduleName) {
            model.setModuleName(moduleName);
            pmml.setModuleName(moduleName);
            return this;
        }

        public PMMLModelWorkflowConfiguration build() {
            configuration.setContainerConfiguration("pmmlModelWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            configuration.add(pmml);
            configuration.add(model);
            configuration.add(exportModelToS3);
            return configuration;
        }

    }
}
