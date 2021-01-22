package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.latticeengines.apps.cdl.integration.Broker;
import com.latticeengines.apps.cdl.integration.MockBroker;
import com.latticeengines.apps.cdl.service.InboundConnectionService;
import com.latticeengines.apps.cdl.service.MockBrokerInstanceService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.cdl.IngestionScheduler;
import com.latticeengines.domain.exposed.cdl.integration.BrokerReference;
import com.latticeengines.domain.exposed.cdl.integration.InboundConnectionType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class InboundConnectionServiceImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(InboundConnectionServiceImplTestNG.class);

    @Inject
    private InboundConnectionService inboundConnectionService;

    @Inject
    private MockBrokerInstanceService mockBrokerInstanceService;

    private RetryTemplate retry;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
        retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AssertionError.class), null);
    }

    @Test(groups = "functional")
    public void testBrokerFactory() {
        BrokerReference brokerReference = new BrokerReference();
        brokerReference.setConnectionType(InboundConnectionType.Mock);
        List<String> selectedFields = new ArrayList<>();
        selectedFields.addAll(Lists.newArrayList(InterfaceName.AccountId.name(), InterfaceName.City.name(), InterfaceName.PhoneNumber.name()));
        brokerReference.setSelectedFields(selectedFields);
        brokerReference.setDocumentType(BusinessEntity.Account.name());
        brokerReference = inboundConnectionService.setUpBroker(brokerReference);
        Broker broker = inboundConnectionService.getBroker(brokerReference);
        Assert.assertTrue(broker instanceof MockBroker);
        String sourceId = brokerReference.getSourceId();
        Assert.assertNotNull(sourceId);
        List<String> entities = broker.listDocumentTypes();
        Assert.assertTrue(entities.contains(BusinessEntity.Account.name()));
        Assert.assertTrue(entities.contains(BusinessEntity.Contact.name()));
        Assert.assertEquals(broker.describeDocumentType(BusinessEntity.Account.name()).size(), 7);
        Assert.assertEquals(broker.describeDocumentType(BusinessEntity.Contact.name()).size(), 6);
        broker.start();
        retry.execute(context -> {
            BrokerReference brokerReference1 = broker.getBrokerReference();
            Assert.assertNotNull(brokerReference1);
            List<String> selectedFields2 = brokerReference1.getSelectedFields();
            Assert.assertEquals(selectedFields2.size(), 3);
            Assert.assertTrue(brokerReference1.getActive());
            IngestionScheduler savedScheduler = brokerReference1.getScheduler();
            Assert.assertNull(savedScheduler);
            return true;
        });
    }

}
