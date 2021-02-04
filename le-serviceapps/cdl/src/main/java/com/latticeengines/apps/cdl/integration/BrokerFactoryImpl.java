package com.latticeengines.apps.cdl.integration;

import javax.inject.Inject;

import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.MockBrokerInstanceService;
import com.latticeengines.domain.exposed.cdl.MockBrokerInstance;
import com.latticeengines.domain.exposed.cdl.integration.BrokerReference;
import com.latticeengines.domain.exposed.cdl.integration.InboundConnectionType;

@Component("brokerFactory")
public class BrokerFactoryImpl implements BrokerFactory {

    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private MockBrokerInstanceService mockBrokerInstanceService;

    @Override
    public BrokerReference setUpBroker(BrokerReference brokerReference) {
        InboundConnectionType connectionType = brokerReference.getConnectionType();
        if (connectionType != null) {
            switch (connectionType) {
                case Mock:
                    MockBrokerInstance mockBrokerInstance = new MockBrokerInstance();
                    mockBrokerInstance.setSelectedFields(brokerReference.getSelectedFields());
                    mockBrokerInstance.setDocumentType(brokerReference.getDocumentType());
                    mockBrokerInstance = mockBrokerInstanceService.createOrUpdate(mockBrokerInstance);
                    brokerReference.setSourceId(mockBrokerInstance.getSourceId());
                    return brokerReference;
                default:
                    throw new RuntimeException("Inbound connection type is wrong, can't setup broker.");
            }
        } else {
            throw new RuntimeException("Inbound connection type is empty, can't setup broker.");
        }
    }

    @Override
    public Broker getBroker(BrokerReference brokerReference) {
        InboundConnectionType connectionType = brokerReference.getConnectionType();
        if (connectionType != null) {
            switch (connectionType) {
                case Mock:
                    return applicationContext.getBean(MockBroker.class, brokerReference);
                default:
                    throw new RuntimeException("Broker reference info is wrong, can't get broker.");
            }
        } else {
            throw new RuntimeException("Broker reference info is wrong, can't get broker.");
        }
    }

}
