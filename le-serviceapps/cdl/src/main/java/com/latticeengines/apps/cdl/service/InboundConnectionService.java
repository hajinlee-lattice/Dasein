package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.apps.cdl.integration.Broker;
import com.latticeengines.domain.exposed.cdl.integration.BrokerReference;
import com.latticeengines.domain.exposed.cdl.integration.BrokerSetupInfo;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface InboundConnectionService {

    BrokerReference setUpBroker(BrokerSetupInfo brokerSetupInfo);

    Broker getBroker(BrokerReference brokerReference);

    List<BusinessEntity> listDocumentTypes(BrokerReference brokerReference);

    List<ColumnMetadata> describeDocumentType(BrokerReference brokerReference, BusinessEntity entity);
}
