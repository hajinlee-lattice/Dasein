package com.latticeengines.apps.cdl.tray.service;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;

public interface TrayConnectorTestService {

    void triggerTrayConnectorTest(CDLExternalSystemName externalSystemName, String testScenario);

}
