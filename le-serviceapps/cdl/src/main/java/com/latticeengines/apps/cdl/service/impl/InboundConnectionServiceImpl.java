package com.latticeengines.apps.cdl.service.impl;

import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.integration.Broker;
import com.latticeengines.apps.cdl.integration.BrokerFactory;
import com.latticeengines.apps.cdl.service.InboundConnectionService;
import com.latticeengines.domain.exposed.cdl.integration.BrokerReference;
import com.latticeengines.domain.exposed.cdl.integration.BrokerSetupInfo;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("inboundConnectionService")
public class InboundConnectionServiceImpl implements InboundConnectionService {

    private static final Logger log = LoggerFactory.getLogger(InboundConnectionServiceImpl.class);

    @Inject
    private BrokerFactory brokerFactory;

    @Override
    public BrokerReference setUpBroker(BrokerSetupInfo brokerSetupInfo) {
        return brokerFactory.setUpBroker(brokerSetupInfo);
    }

    @Override
    public Broker getBroker(BrokerReference brokerReference) {
        return brokerFactory.getBroker(brokerReference);
    }

    @Override
    public List<BusinessEntity> listDocumentTypes(BrokerReference brokerReference) {
        Broker broker = brokerFactory.getBroker(brokerReference);
        if (broker != null) {
            return broker.listDocumentTypes();
        } else {
            return null;
        }
    }

    @Override
    public List<ColumnMetadata> describeDocumentType(BrokerReference brokerReference, BusinessEntity entity) {
        Broker broker = brokerFactory.getBroker(brokerReference);
        if (broker != null) {
            return broker.describeDocumentType(entity);
        } else {
            return Collections.emptyList();
        }
    }
}
