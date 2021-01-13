package com.latticeengines.apps.cdl.service.impl;

import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.MockBrokerInstanceEntityMgr;
import com.latticeengines.apps.cdl.service.MockBrokerInstanceService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.MockBrokerInstance;

@Component("mockBrokerInstanceServiceImpl")
public class MockBrokerInstanceServiceImpl implements MockBrokerInstanceService {

    private static final Logger log = LoggerFactory.getLogger(MockBrokerInstanceServiceImpl.class);

    @Inject
    private MockBrokerInstanceEntityMgr mockBrokerInstanceEntityMgr;

    @Override
    public List<MockBrokerInstance> getAllInstance(int maxRow) {
        return mockBrokerInstanceEntityMgr.getAllInstance(maxRow);
    }

    @Override
    public MockBrokerInstance createOrUpdate(MockBrokerInstance existingMockBrokerInstance) {
        String sourceId = existingMockBrokerInstance.getSourceId();
        MockBrokerInstance mockBrokerInstance = null;
        if (StringUtils.isNotEmpty(sourceId)) {
            mockBrokerInstance = mockBrokerInstanceEntityMgr.findBySourceId(sourceId);
        }
        if (mockBrokerInstance == null) {
            mockBrokerInstance = new MockBrokerInstance();
            mockBrokerInstance.setSourceId(UUID.randomUUID().toString());
        }
        mockBrokerInstance.setTenant(MultiTenantContext.getTenant());
        mockBrokerInstance.setDisplayName(existingMockBrokerInstance.getDisplayName());
        if (MapUtils.isNotEmpty(existingMockBrokerInstance.getSelectedFields())) {
            mockBrokerInstance.setSelectedFields(existingMockBrokerInstance.getSelectedFields());
        }
        mockBrokerInstanceEntityMgr.createOrUpdate(mockBrokerInstance);
        return mockBrokerInstance;
    }

    @Override
    public MockBrokerInstance findBySourceId(String sourceId) {
        return mockBrokerInstanceEntityMgr.findBySourceId(sourceId);
    }


}
