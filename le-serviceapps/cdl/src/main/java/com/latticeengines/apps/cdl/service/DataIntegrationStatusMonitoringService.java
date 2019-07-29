package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMessage;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;

public interface DataIntegrationStatusMonitoringService {

    Map<String, Boolean> createOrUpdateStatuses(List<DataIntegrationStatusMonitorMessage> statuses);

    DataIntegrationStatusMonitor getStatus(String eventId);

    List<DataIntegrationStatusMonitor> getAllStatuses(String tenantId);

    List<DataIntegrationStatusMessage> getAllStatusMessages(String workflowRequestId);

    List<DataIntegrationStatusMonitor> getAllStatusesByEntityNameAndIds(String tenantId, String entityName, List<String> entityIds);

}
