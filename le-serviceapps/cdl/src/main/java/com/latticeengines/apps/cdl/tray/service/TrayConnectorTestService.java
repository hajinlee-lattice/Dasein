package com.latticeengines.apps.cdl.tray.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTest;

public interface TrayConnectorTestService {

    void triggerTrayConnectorTest(String customerSpace, CDLExternalSystemName externalSystemName, String testScenario);

    Map<String, Boolean> verifyTrayConnectorTest(List<DataIntegrationStatusMonitorMessage> statuses);

    List<TrayConnectorTest> findUnfinishedTests();

    void cancelTrayTestByWorkflowReqId(String workflowRequestId);

    boolean isAdPlatform(TrayConnectorTest test);

    boolean isLiveramp(TrayConnectorTest test);
}
