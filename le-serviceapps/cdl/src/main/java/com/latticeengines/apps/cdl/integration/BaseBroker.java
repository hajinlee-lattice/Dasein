package com.latticeengines.apps.cdl.integration;

import com.latticeengines.domain.exposed.cdl.integration.BrokerReference;

public abstract class BaseBroker implements Broker {

    protected String sourceId;

    protected BaseBroker(BrokerReference brokerReference) {
        this.sourceId = brokerReference.getSourceId();
    }

}
