package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMessage;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;

public interface DataIntegrationStatusMonitoringService {

    boolean createOrUpdateStatus(DataIntegrationStatusMonitorMessage status);

    DataIntegrationStatusMonitor getStatus(String eventId);

    List<DataIntegrationStatusMonitor> getAllStatuses(String tenantId);

    List<DataIntegrationStatusMessage> getAllStatusMessages(String workflowRequestId);

    List<DataIntegrationStatusMonitor> getAllStatusesByEntityNameAndIds(String tenantId, String entityName, List<String> entityIds);

}
