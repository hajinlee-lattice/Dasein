package com.latticeengines.apps.cdl.service.impl;

import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.MockBrokerInstanceEntityMgr;
import com.latticeengines.apps.cdl.service.MockBrokerInstanceService;
import com.latticeengines.common.exposed.util.CronUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.IngestionScheduler;
import com.latticeengines.domain.exposed.cdl.MockBrokerInstance;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("mockBrokerInstanceServiceImpl")
public class MockBrokerInstanceServiceImpl implements MockBrokerInstanceService {

    private static final Logger log = LoggerFactory.getLogger(MockBrokerInstanceServiceImpl.class);

    @Inject
    private MockBrokerInstanceEntityMgr mockBrokerInstanceEntityMgr;

    private String defaultDisplayName = "MockBroker";

    @Override
    public List<MockBrokerInstance> getAllInstance(int maxRow) {
        return mockBrokerInstanceEntityMgr.getAllInstance(maxRow);
    }

    @Override
    public List<MockBrokerInstance> getAllValidInstance() {
        return mockBrokerInstanceEntityMgr.getAllValidInstance();
    }

    @Override
    public MockBrokerInstance createOrUpdate(MockBrokerInstance existingMockBrokerInstance) {
        validate(existingMockBrokerInstance);
        String sourceId = existingMockBrokerInstance.getSourceId();
        MockBrokerInstance mockBrokerInstance = null;
        if (StringUtils.isNotEmpty(sourceId)) {
            mockBrokerInstance = mockBrokerInstanceEntityMgr.findBySourceId(sourceId);
        }
        if (mockBrokerInstance == null) {
            mockBrokerInstance = new MockBrokerInstance();
            mockBrokerInstance.setSourceId(UUID.randomUUID().toString());
            if (StringUtils.isNotEmpty(existingMockBrokerInstance.getDocumentType())) {
                mockBrokerInstance.setDocumentType(existingMockBrokerInstance.getDocumentType());
            } else {
                mockBrokerInstance.setDocumentType("Account");
            }
        }
        mockBrokerInstance.setTenant(MultiTenantContext.getTenant());
        if (StringUtils.isNotEmpty(existingMockBrokerInstance.getDisplayName())) {
            mockBrokerInstance.setDisplayName(existingMockBrokerInstance.getDisplayName());
        } else {
            mockBrokerInstance.setDisplayName(defaultDisplayName);
        }
        if (CollectionUtils.isNotEmpty(existingMockBrokerInstance.getSelectedFields())) {
            mockBrokerInstance.setSelectedFields(existingMockBrokerInstance.getSelectedFields());
        }
        if (existingMockBrokerInstance.getIngestionScheduler() != null) {
            mockBrokerInstance.setIngestionScheduler(existingMockBrokerInstance.getIngestionScheduler());
        }
        mockBrokerInstance.setActive(existingMockBrokerInstance.getActive());
        mockBrokerInstanceEntityMgr.createOrUpdate(mockBrokerInstance);
        return mockBrokerInstance;
    }

    private void validate(MockBrokerInstance mockBrokerInstance) {
        IngestionScheduler scheduler = mockBrokerInstance.getIngestionScheduler();
        if (scheduler != null) {
            String cronExpression = scheduler.getCronExpression();
            if (StringUtils.isEmpty(cronExpression) || !CronUtils.isValidExpression(cronExpression)) {
                throw new LedpException(LedpCode.LEDP_41004);
            }
        }
    }

    @Override
    public MockBrokerInstance findBySourceId(String sourceId) {
        return mockBrokerInstanceEntityMgr.findBySourceId(sourceId);
    }


}
