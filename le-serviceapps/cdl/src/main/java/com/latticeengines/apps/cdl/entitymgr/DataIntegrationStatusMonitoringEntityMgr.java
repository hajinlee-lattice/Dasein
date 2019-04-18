package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;

public interface DataIntegrationStatusMonitoringEntityMgr {

    boolean createStatus(DataIntegrationStatusMonitor status);

    DataIntegrationStatusMonitor getStatus(String eventId);

    DataIntegrationStatusMonitor updateStatus(DataIntegrationStatusMonitor status);

    List<DataIntegrationStatusMonitor> getAllStatuses(Long tenantId);

    List<DataIntegrationStatusMonitor> getAllStatusesByEntityNameAndIds(Long tenantPid, String entityName,
            List<String> entityIds);
}
