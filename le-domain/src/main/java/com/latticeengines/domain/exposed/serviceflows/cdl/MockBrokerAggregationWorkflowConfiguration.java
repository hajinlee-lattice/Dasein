package com.latticeengines.domain.exposed.serviceflows.cdl;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.integration.AggregateMockInstanceFileConfiguration;

public class MockBrokerAggregationWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public MockBrokerAggregationWorkflowConfiguration() {
    }

    public static class Builder {
        private MockBrokerAggregationWorkflowConfiguration configuration = new MockBrokerAggregationWorkflowConfiguration();
        private AggregateMockInstanceFileConfiguration aggregateMockInstanceFileConfiguration = new AggregateMockInstanceFileConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("mockBrokerAggregationWorkflow", customerSpace, configuration.getClass().getSimpleName());
            aggregateMockInstanceFileConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public MockBrokerAggregationWorkflowConfiguration build() {
            configuration.add(aggregateMockInstanceFileConfiguration);
            return configuration;
        }
    }
}
