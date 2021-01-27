package com.latticeengines.proxy.exposed.cdl;

import com.latticeengines.domain.exposed.cdl.integration.BrokerReference;

public interface InboundConnectionProxy {

    BrokerReference getBrokerReference(String customerSpace, BrokerReference brokerReference);

    BrokerReference updateBroker(String customerSpace, BrokerReference brokerReference);
}

