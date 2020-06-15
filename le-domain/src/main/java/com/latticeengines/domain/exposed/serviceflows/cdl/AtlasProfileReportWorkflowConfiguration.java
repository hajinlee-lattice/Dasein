package com.latticeengines.domain.exposed.serviceflows.cdl;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.report.ProfileReportStepConfiguration;

public class AtlasProfileReportWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static final String WORKFLOW_NAME = "atlasProfileReportWorkflow";

    public static class Builder {

        private AtlasProfileReportWorkflowConfiguration configuration = new AtlasProfileReportWorkflowConfiguration();
        private ProfileReportStepConfiguration step = new ProfileReportStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            step.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder userId(String userId) {
            configuration.setUserId(userId);
            step.setUserId(userId);
            return this;
        }

        public Builder allowInternalEnrichAttrs(boolean allowInternalEnrichAttrs) {
            step.setAllowInternalEnrichAttrs(allowInternalEnrichAttrs);
            return this;
        }

        public AtlasProfileReportWorkflowConfiguration build() {
            configuration.setContainerConfiguration(WORKFLOW_NAME, configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(step);
            return configuration;
        }

    }

}
