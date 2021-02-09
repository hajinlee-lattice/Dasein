package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.apps.cdl.integration.Broker;
import com.latticeengines.domain.exposed.cdl.integration.BrokerReference;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;

public interface InboundConnectionService {

    BrokerReference setUpBroker(BrokerReference brokerReference);

    Broker getBroker(BrokerReference brokerReference);

    List<String> listDocumentTypes(BrokerReference brokerReference);

    List<ColumnMetadata> describeDocumentType(BrokerReference brokerReference, String documentType);

    void submitMockBrokerAggregationWorkflow();

    BrokerReference getBrokerReference(BrokerReference brokerReference);

    void schedule(BrokerReference brokerReference);

    BrokerReference updateBroker(BrokerReference brokerReference);

}
