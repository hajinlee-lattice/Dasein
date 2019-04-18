package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMessage;

public interface DataIntegrationStatusMessageEntityMgr {

    void create(DataIntegrationStatusMessage statusMessage);

    List<DataIntegrationStatusMessage> getAllStatusMessages(Long statusMonitorPid);

}
