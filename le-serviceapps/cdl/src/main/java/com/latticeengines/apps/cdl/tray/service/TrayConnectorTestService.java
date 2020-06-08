package com.latticeengines.apps.cdl.tray.service;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;

public interface TrayConnectorTestService {

    public void createTrayConnectorTest(CDLExternalSystemName externalSystemName, String testScenario);

}
