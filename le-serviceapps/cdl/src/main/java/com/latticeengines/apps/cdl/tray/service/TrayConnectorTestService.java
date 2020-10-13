package com.latticeengines.apps.cdl.tray.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTest;

public interface TrayConnectorTestService {

    void triggerTrayConnectorTest(String customerSpace, CDLExternalSystemName externalSystemName, String testScenario);

    void verifyTrayConnectorTest(List<DataIntegrationStatusMonitorMessage> statuses);

    List<TrayConnectorTest> findUnfinishedTests();

    void cancelTrayTestByWorkflowReqId(String workflowRequestId);

    boolean isAdPlatform(TrayConnectorTest test);
}
