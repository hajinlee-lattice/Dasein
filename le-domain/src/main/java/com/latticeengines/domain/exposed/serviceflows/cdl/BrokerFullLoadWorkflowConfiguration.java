package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Date;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.integration.InboundConnectionType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.integration.BrokerDataFullLoadConfiguration;

public class BrokerFullLoadWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public BrokerFullLoadWorkflowConfiguration() {
    }

    public static class Builder {
        private BrokerFullLoadWorkflowConfiguration configuration = new BrokerFullLoadWorkflowConfiguration();
        private BrokerDataFullLoadConfiguration brokerDataFullLoadConfiguration = new BrokerDataFullLoadConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("brokerFullLoadWorkflow", customerSpace, configuration.getClass().getSimpleName());
            brokerDataFullLoadConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder startTime(Date startTime) {
            brokerDataFullLoadConfiguration.setStartTime(startTime);
            return this;
        }

        public Builder endTime(Date endTime) {
            brokerDataFullLoadConfiguration.setEndTime(endTime);
            return this;
        }

        public Builder inboundConnectionType(InboundConnectionType inboundConnectionType) {
            brokerDataFullLoadConfiguration.setInboundConnectionType(inboundConnectionType);
            return this;
        }

        public Builder sourceId(String sourceId) {
            brokerDataFullLoadConfiguration.setSourceId(sourceId);
            return this;
        }

        public Builder bucket(String bucket) {
            brokerDataFullLoadConfiguration.setBucket(bucket);
            return this;
        }

        public BrokerFullLoadWorkflowConfiguration build() {
            configuration.add(brokerDataFullLoadConfiguration);
            return configuration;
        }
    }
}
