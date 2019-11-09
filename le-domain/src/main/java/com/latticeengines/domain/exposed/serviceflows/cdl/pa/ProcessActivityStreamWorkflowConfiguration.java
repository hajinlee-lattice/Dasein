package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.activity.ActivityImport;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.GenerateDimensionMetadataStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessActivityStreamStepConfiguration;

public class ProcessActivityStreamWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private ProcessActivityStreamWorkflowConfiguration configuration = new ProcessActivityStreamWorkflowConfiguration();
        private ProcessActivityStreamStepConfiguration processStepConfiguration = new ProcessActivityStreamStepConfiguration();
        private GenerateDimensionMetadataStepConfiguration generateDimMetadataConfig = new GenerateDimensionMetadataStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            processStepConfiguration.setCustomerSpace(customerSpace);
            generateDimMetadataConfig.setCustomer(customerSpace.toString());
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            processStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            generateDimMetadataConfig.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder entityMatchEnabled(boolean entityMatchEnabled) {
            processStepConfiguration.setSkipStep(!entityMatchEnabled);
            generateDimMetadataConfig.setSkipStep(!entityMatchEnabled);
            processStepConfiguration.setEntityMatchEnabled(entityMatchEnabled);
            generateDimMetadataConfig.setEntityMatchEnabled(entityMatchEnabled);
            return this;
        }

        public Builder entityMatchGAOnly(boolean entityMatchGAOnly) {
            processStepConfiguration.setEntityMatchGAOnly(entityMatchGAOnly);
            generateDimMetadataConfig.setEntityMatchGAOnly(entityMatchGAOnly);
            return this;
        }

        public Builder activeRawStreamTables(Map<String, String> rawStreamTables) {
            processStepConfiguration.setActiveRawStreamTables(rawStreamTables);
            return this;
        }

        public Builder activityStreams(Map<String, AtlasStream> activityStreams) {
            processStepConfiguration.setActivityStreamMap(activityStreams);
            generateDimMetadataConfig.setActivityStreamMap(activityStreams);
            return this;
        }

        public Builder activityStreamImports(Map<String, List<ActivityImport>> activityStreamImports) {
            processStepConfiguration.setStreamImports(activityStreamImports);
            return this;
        }

        public ProcessActivityStreamWorkflowConfiguration build() {
            configuration.setContainerConfiguration("processActivityStreamWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            configuration.add(processStepConfiguration);
            configuration.add(generateDimMetadataConfig);
            return configuration;
        }
    }
}
