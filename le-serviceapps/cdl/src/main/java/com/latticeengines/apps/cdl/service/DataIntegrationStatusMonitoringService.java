package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;

public interface DataIntegrationStatusMonitoringService {
    public boolean createOrUpdateStatus(DataIntegrationStatusMonitorMessage status);

    public DataIntegrationStatusMonitor getStatus(String eventId);

    public List<DataIntegrationStatusMonitor> getAllStatuses(Long tenantId);

}
