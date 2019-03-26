package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMessage;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;

public interface DataIntegrationStatusMonitoringService {
    public boolean createOrUpdateStatus(DataIntegrationStatusMonitorMessage status);

    public DataIntegrationStatusMonitor getStatus(String eventId);

    public List<DataIntegrationStatusMonitor> getAllStatuses(String tenantId);

    public List<DataIntegrationStatusMessage> getAllStatusMessages(String workflowRequestId);

    public List<DataIntegrationStatusMonitor> getAllStatusesByEntityNameAndIds(String tenantId, String entityName, List<String> entityIds);
}
