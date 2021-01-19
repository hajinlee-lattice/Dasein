package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;

public class GenerateVisitReportWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {

        private GenerateVisitReportWorkflowConfiguration configuration = new GenerateVisitReportWorkflowConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public GenerateVisitReportWorkflowConfiguration build() {
            configuration.setContainerConfiguration("generateVisitReportWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            return configuration;
        }
    }
}
