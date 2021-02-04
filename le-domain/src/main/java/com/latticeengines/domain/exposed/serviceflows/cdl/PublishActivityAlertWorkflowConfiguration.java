package com.latticeengines.domain.exposed.serviceflows.cdl;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.TimeLineSparkStepConfiguration;

public class PublishActivityAlertWorkflowConfiguration extends BaseCDLWorkflowConfiguration {
    public static final String NAME = "PublishActivityAlertWorkflowConfiguration";
    public static final String WORKFLOW_NAME = "publishActivityAlertWorkflow";

    public static class Builder {
        private PublishActivityAlertWorkflowConfiguration configuration = new PublishActivityAlertWorkflowConfiguration();
        private TimeLineSparkStepConfiguration publishActivityAlertConfig = new TimeLineSparkStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            publishActivityAlertConfig.setCustomer(customerSpace.toString());
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            publishActivityAlertConfig.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder setRebuild(Boolean rebuild) {
            publishActivityAlertConfig.setShouldRebuild(rebuild);
            return this;
        }

        public PublishActivityAlertWorkflowConfiguration build() {
            configuration.setContainerConfiguration(WORKFLOW_NAME, configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(publishActivityAlertConfig);

            return configuration;
        }
    }
}
