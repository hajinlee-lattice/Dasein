package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.MockBrokerInstance;

public interface MockBrokerInstanceService {

    List<MockBrokerInstance> getAllInstance(int maxRow);

    List<MockBrokerInstance> getAllValidInstance();

    MockBrokerInstance createOrUpdate(MockBrokerInstance mockBrokerInstance);

    MockBrokerInstance findBySourceId(String sourceId);

}
