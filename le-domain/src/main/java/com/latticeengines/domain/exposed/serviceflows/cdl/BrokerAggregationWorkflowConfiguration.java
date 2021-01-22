package com.latticeengines.domain.exposed.serviceflows.cdl;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.integration.AggregateBrokerFileConfiguration;

public class BrokerAggregationWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public BrokerAggregationWorkflowConfiguration() {
    }

    public static class Builder {
        private BrokerAggregationWorkflowConfiguration configuration = new BrokerAggregationWorkflowConfiguration();
        private AggregateBrokerFileConfiguration aggregateBrokerFileConfiguration = new AggregateBrokerFileConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("brokerAggregationWorkflow", customerSpace, configuration.getClass().getSimpleName());
            aggregateBrokerFileConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public BrokerAggregationWorkflowConfiguration build() {
            configuration.add(aggregateBrokerFileConfiguration);
            return configuration;
        }
    }
}
