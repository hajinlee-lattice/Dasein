package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Date;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.integration.InboundConnectionType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.integration.BrokerDataInitialLoadConfiguration;

public class BrokerInitialLoadWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public BrokerInitialLoadWorkflowConfiguration() {
    }

    public static class Builder {
        private BrokerInitialLoadWorkflowConfiguration configuration = new BrokerInitialLoadWorkflowConfiguration();
        private BrokerDataInitialLoadConfiguration brokerDataInitialLoadConfiguration = new BrokerDataInitialLoadConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("brokerInitialLoadWorkflow", customerSpace, configuration.getClass().getSimpleName());
            brokerDataInitialLoadConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder startTime(Date startTime) {
            brokerDataInitialLoadConfiguration.setStartTime(startTime);
            return this;
        }

        public Builder endTime(Date endTime) {
            brokerDataInitialLoadConfiguration.setEndTime(endTime);
            return this;
        }

        public Builder inboundConnectionType(InboundConnectionType inboundConnectionType) {
            brokerDataInitialLoadConfiguration.setInboundConnectionType(inboundConnectionType);
            return this;
        }

        public Builder sourceId(String sourceId) {
            brokerDataInitialLoadConfiguration.setSourceId(sourceId);
            return this;
        }

        public Builder bucket(String bucket) {
            brokerDataInitialLoadConfiguration.setBucket(bucket);
            return this;
        }

        public BrokerInitialLoadWorkflowConfiguration build() {
            configuration.add(brokerDataInitialLoadConfiguration);
            return configuration;
        }
    }
}
