package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.MockBrokerInstance;

public interface MockBrokerInstanceEntityMgr extends BaseEntityMgrRepository<MockBrokerInstance, Long> {

    MockBrokerInstance findBySourceId(String sourceId);

    List<MockBrokerInstance> getAllInstance(int maxRow);

}
