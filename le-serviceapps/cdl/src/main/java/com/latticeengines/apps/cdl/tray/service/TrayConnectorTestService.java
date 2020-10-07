package com.latticeengines.apps.cdl.tray.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;

public interface TrayConnectorTestService {

    void triggerTrayConnectorTest(CDLExternalSystemName externalSystemName, String testScenario);

    void verifyTrayConnectorTest(List<DataIntegrationStatusMonitorMessage> statuses);
}
