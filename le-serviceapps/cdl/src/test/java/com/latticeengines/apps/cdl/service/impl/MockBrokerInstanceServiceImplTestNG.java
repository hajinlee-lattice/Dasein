package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.latticeengines.apps.cdl.service.MockBrokerInstanceService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.cdl.MockBrokerInstance;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class MockBrokerInstanceServiceImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(MockBrokerInstanceServiceImplTestNG.class);

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
    public void testCRUD() {
        String sourceId = UUID.randomUUID().toString();
        String displayName = "mockBrokerInstance";
        AtomicReference<MockBrokerInstance> mockBrokerInstance = new AtomicReference<>(new MockBrokerInstance());
        mockBrokerInstance.get().setDisplayName(displayName);
        mockBrokerInstance.get().setSourceId(sourceId);
        mockBrokerInstance.get().getSelectedFields().put(BusinessEntity.Account.name(), new ArrayList<>());
        mockBrokerInstance.get().getSelectedFields().put(BusinessEntity.Contact.name(), new ArrayList<>());
        mockBrokerInstance.get().getSelectedFields().get(BusinessEntity.Account.name()).addAll(Lists.newArrayList(InterfaceName.AccountId.name(),
                InterfaceName.City.name(), InterfaceName.Country.name()));
        mockBrokerInstance.get().getSelectedFields().get(BusinessEntity.Contact.name()).addAll(Lists.newArrayList(InterfaceName.AccountId.name(),
                InterfaceName.ContactId.name(), InterfaceName.Email.name(), InterfaceName.FirstName.name()));
        mockBrokerInstanceService.createOrUpdate(mockBrokerInstance.get());
        retry.execute(context -> {
            mockBrokerInstance.set(mockBrokerInstanceService.findBySourceId(sourceId));
            Assert.assertNotNull(mockBrokerInstance.get());
            Assert.assertEquals(mockBrokerInstance.get().getDisplayName(), displayName);
            Map<String, List<String>> selectedFields = mockBrokerInstance.get().getSelectedFields();
            Assert.assertNotNull(selectedFields);
            Assert.assertEquals(selectedFields.get(BusinessEntity.Account.name()).size(), 3);
            Assert.assertEquals(selectedFields.get(BusinessEntity.Contact.name()).size(), 4);
            return true;
        });
        mockBrokerInstance.get().setDisplayName("mockBrokerInstance1");
        mockBrokerInstanceService.createOrUpdate(mockBrokerInstance.get());
        retry.execute(context -> {
            mockBrokerInstance.set(mockBrokerInstanceService.findBySourceId(sourceId));
            Assert.assertNotNull(mockBrokerInstance.get());
            Assert.assertEquals(mockBrokerInstance.get().getDisplayName(), "mockBrokerInstance1");
            return true;
        });
    }

}
