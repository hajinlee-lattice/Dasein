package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.latticeengines.domain.exposed.cdl.MockBrokerInstance;
import com.latticeengines.domain.exposed.cdl.integration.BrokerReference;
import com.latticeengines.domain.exposed.cdl.integration.BrokerSetupInfo;
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
        BrokerSetupInfo brokerSetupInfo = new BrokerSetupInfo();
        brokerSetupInfo.setConnectionType(InboundConnectionType.Mock);
        Map<String, List<String>> selectedFields = new HashMap<>();
        selectedFields.put(BusinessEntity.Account.name(), new ArrayList<>());
        selectedFields.put(BusinessEntity.Contact.name(), new ArrayList<>());
        selectedFields.get(BusinessEntity.Account.name()).addAll(Lists.newArrayList(InterfaceName.AccountId.name(),
                InterfaceName.City.name(), InterfaceName.PhoneNumber.name()));
        selectedFields.get(BusinessEntity.Contact.name()).addAll(Lists.newArrayList(InterfaceName.AccountId.name(),
                InterfaceName.ContactId.name(), InterfaceName.Email.name(), InterfaceName.FirstName.name()));
        brokerSetupInfo.setSelectedFields(selectedFields);
        BrokerReference brokerReference = inboundConnectionService.setUpBroker(brokerSetupInfo);
        Broker broker = inboundConnectionService.getBroker(brokerReference);
        Assert.assertTrue(broker instanceof MockBroker);
        String sourceId = brokerReference.getSourceId();
        Assert.assertNotNull(sourceId);
        List<BusinessEntity> entities = broker.listDocumentTypes();
        Assert.assertTrue(entities.contains(BusinessEntity.Account));
        Assert.assertTrue(entities.contains(BusinessEntity.Contact));
        Assert.assertEquals(broker.describeDocumentType(BusinessEntity.Account).size(), 8);
        Assert.assertEquals(broker.describeDocumentType(BusinessEntity.Contact).size(), 6);
        broker.start();
        String cronExpression = "0 0/10 * * * ?";
        long startTime = System.currentTimeMillis();
        IngestionScheduler scheduler = new IngestionScheduler();
        scheduler.setCronExpression(cronExpression);
        scheduler.setStartTime(startTime);
        broker.schedule(scheduler);
        retry.execute(context -> {
            MockBrokerInstance mockBrokerInstance = mockBrokerInstanceService.findBySourceId(sourceId);
            Assert.assertNotNull(mockBrokerInstance);
            Map<String, List<String>> selectedFields2 = mockBrokerInstance.getSelectedFields();
            Assert.assertEquals(selectedFields2.get(BusinessEntity.Account.name()).size(), 3);
            Assert.assertEquals(selectedFields2.get(BusinessEntity.Contact.name()).size(), 4);
            Assert.assertTrue(mockBrokerInstance.getActive());
            IngestionScheduler savedScheduler = mockBrokerInstance.getIngestionScheduler();
            Assert.assertNotNull(savedScheduler);
            Assert.assertEquals(savedScheduler.getCronExpression(), cronExpression);
            Assert.assertEquals(savedScheduler.getStartTime(), startTime);
            return true;
        });
    }

}
