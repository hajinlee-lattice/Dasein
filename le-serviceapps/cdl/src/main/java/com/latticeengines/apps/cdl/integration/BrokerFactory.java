package com.latticeengines.apps.cdl.integration;

import com.latticeengines.domain.exposed.cdl.integration.BrokerReference;

public interface BrokerFactory {

    BrokerReference setUpBroker(BrokerReference brokerReference);

    Broker getBroker(BrokerReference brokerReference);
}
