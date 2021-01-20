package com.latticeengines.apps.cdl.integration;

import com.latticeengines.domain.exposed.cdl.integration.BrokerReference;
import com.latticeengines.domain.exposed.cdl.integration.BrokerSetupInfo;

public interface BrokerFactory {

    BrokerReference setUpBroker(BrokerSetupInfo brokerSetupInfo);

    Broker getBroker(BrokerReference brokerReference);
}
