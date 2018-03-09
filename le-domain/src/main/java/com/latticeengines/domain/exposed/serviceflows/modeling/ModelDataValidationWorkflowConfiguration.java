package com.latticeengines.domain.exposed.serviceflows.modeling;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.CreatePrematchEventTableReportConfiguration;

public class ModelDataValidationWorkflowConfiguration extends BaseModelingWorkflowConfiguration {

    public static class Builder {

        private ModelDataValidationWorkflowConfiguration configuration = new ModelDataValidationWorkflowConfiguration();
        private CreatePrematchEventTableReportConfiguration createEventTableReport = new CreatePrematchEventTableReportConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("modelDataValidationWorkflow", customerSpace,
                    configuration.getClass().getSimpleName());
            createEventTableReport.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            createEventTableReport.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            createEventTableReport.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder eventTableReportNamePrefix(String eventTableReportName) {
            createEventTableReport.setReportNamePrefix(eventTableReportName);
            return this;
        }

        public Builder sourceTableName(String eventTableReportSourceFileName) {
            createEventTableReport.setSourceTableName(eventTableReportSourceFileName);
            return this;
        }

        public Builder minPositiveEvents(long minPositiveEvents) {
            createEventTableReport.setMinPositiveEvents(minPositiveEvents);
            return this;
        }

        public Builder minNegativeEvents(long minNegativeEvents) {
            createEventTableReport.setMinNegativeEvents(minNegativeEvents);
            return this;
        }

        public Builder minRows(long minRows) {
            createEventTableReport.setMinRows(minRows);
            return this;
        }

        public ModelDataValidationWorkflowConfiguration build() {
            configuration.add(createEventTableReport);
            return configuration;
        }
    }
}
