package com.latticeengines.apps.cdl.service.impl;

import java.util.Collections;
import java.util.Date;
import java.util.List;
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
import com.latticeengines.domain.exposed.cdl.IngestionScheduler;
import com.latticeengines.domain.exposed.cdl.MockBrokerInstance;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

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
        String displayName = "mockBrokerInstance";
        AtomicReference<MockBrokerInstance> mockBrokerInstance = new AtomicReference<>(new MockBrokerInstance());
        mockBrokerInstance.get().setDisplayName(displayName);
        mockBrokerInstance.get().getSelectedFields().addAll(Lists.newArrayList(InterfaceName.AccountId.name(),
                InterfaceName.City.name(), InterfaceName.PhoneNumber.name()));
        mockBrokerInstance.get().setDocumentType("Account");
        MockBrokerInstance mockBrokerInstance2 = mockBrokerInstanceService.createOrUpdate(mockBrokerInstance.get());
        String sourceId = mockBrokerInstance2.getSourceId();
        retry.execute(context -> {
            mockBrokerInstance.set(mockBrokerInstanceService.findBySourceId(sourceId));
            Assert.assertNotNull(mockBrokerInstance.get());
            Assert.assertEquals(mockBrokerInstance.get().getDisplayName(), displayName);
            List<String> selectedFields = mockBrokerInstance.get().getSelectedFields();
            Assert.assertNotNull(selectedFields);
            Assert.assertEquals(selectedFields.size(), 3);
            return true;
        });
        String displayName1 = "mockBrokerInstance1";
        mockBrokerInstance.get().setDisplayName(displayName1);
        String cronExpression = "0 0/10 * * * ?";
        Date startTime = new Date(System.currentTimeMillis());
        IngestionScheduler scheduler = new IngestionScheduler();
        scheduler.setCronExpression(cronExpression);
        scheduler.setStartTime(startTime);
        mockBrokerInstance.get().setIngestionScheduler(scheduler);
        mockBrokerInstance.get().setActive(true);
        mockBrokerInstanceService.createOrUpdate(mockBrokerInstance.get());
        retry.execute(context -> {
            mockBrokerInstance.set(mockBrokerInstanceService.findBySourceId(sourceId));
            Assert.assertNotNull(mockBrokerInstance.get());
            Assert.assertEquals(mockBrokerInstance.get().getDisplayName(), displayName1);
            IngestionScheduler savedScheduler = mockBrokerInstance.get().getIngestionScheduler();
            Assert.assertNotNull(savedScheduler);
            Assert.assertEquals(savedScheduler.getCronExpression(), cronExpression);
            Assert.assertEquals(savedScheduler.getStartTime(), startTime);
            Assert.assertTrue(mockBrokerInstance.get().getActive());
            return true;
        });
    }

}
