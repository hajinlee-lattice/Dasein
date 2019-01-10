package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMessage;

public interface DataIntegrationStatusMessageEntityMgr {

    public void create(DataIntegrationStatusMessage statusMessage);

    public List<DataIntegrationStatusMessage> getAllStatusMessages(String eventId);
}
