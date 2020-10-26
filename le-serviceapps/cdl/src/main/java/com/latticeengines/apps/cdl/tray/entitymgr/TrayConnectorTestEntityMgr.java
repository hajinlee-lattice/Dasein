package com.latticeengines.apps.cdl.tray.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTest;

public interface TrayConnectorTestEntityMgr extends BaseEntityMgrRepository<TrayConnectorTest, Long> {

    void create(TrayConnectorTest trayConnectorTest);

    void deleteByWorkflowRequestId(String workflowRequestId);

    TrayConnectorTest findByWorkflowRequestId(String workflowRequestId);

    TrayConnectorTest updateTrayConnectorTest(TrayConnectorTest trayConnectorTest);

    List<TrayConnectorTest> findUnfinishedTests();

}
