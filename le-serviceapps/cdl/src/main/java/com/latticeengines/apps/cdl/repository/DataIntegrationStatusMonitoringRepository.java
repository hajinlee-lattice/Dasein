package com.latticeengines.apps.cdl.repository;

import java.util.List;

import org.springframework.data.jpa.repository.Query;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMessage;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;

public interface DataIntegrationStatusMonitoringRepository
        extends BaseJpaRepository<DataIntegrationStatusMonitor, Long> {

    DataIntegrationStatusMonitor findByWorkflowRequestId(String workflowRequestId);

    List<DataIntegrationStatusMonitor> findAllByTenantPid(Long tenantPid);

    List<DataIntegrationStatusMonitor> findAllByTenantPidAndEntityNameAndEntityIdIn(Long tenantPid, String entityName,
            List<String> entityIds);

    @Query("select msg from DataIntegrationStatusMessage msg " +
            "where msg.statusMonitor.entityId = ?1 " +
            "order by msg.updatedDate desc")
    List<DataIntegrationStatusMessage> getMessagesByLaunchId(String launchId);
}
