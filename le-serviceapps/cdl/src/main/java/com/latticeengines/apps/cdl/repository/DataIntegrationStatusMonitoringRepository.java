package com.latticeengines.apps.cdl.repository;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;

public interface DataIntegrationStatusMonitoringRepository
        extends BaseJpaRepository<DataIntegrationStatusMonitor, Long> {

    DataIntegrationStatusMonitor findByWorkflowRequestId(String workflowRequestId);

    List<DataIntegrationStatusMonitor> findAllByTenantPid(Long tenantPid);

    List<DataIntegrationStatusMonitor> findAllByTenantPidAndEntityNameAndEntityIdIn(Long tenantPid, String entityName,
            List<String> entityIds);

}
