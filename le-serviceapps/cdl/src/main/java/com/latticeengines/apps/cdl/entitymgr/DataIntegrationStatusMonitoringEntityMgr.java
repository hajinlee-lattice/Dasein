package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;

public interface DataIntegrationStatusMonitoringEntityMgr {

    public boolean createStatus(DataIntegrationStatusMonitor status);

    public DataIntegrationStatusMonitor getStatus(String eventId);

    public DataIntegrationStatusMonitor updateStatus(DataIntegrationStatusMonitor status);

    public List<DataIntegrationStatusMonitor> getAllStatuses(Long tenantId);

    public List<DataIntegrationStatusMonitor> getAllStatusesByEntityNameAndIds(Long tenantPid, String entityName,
            List<String> entityIds);
}
