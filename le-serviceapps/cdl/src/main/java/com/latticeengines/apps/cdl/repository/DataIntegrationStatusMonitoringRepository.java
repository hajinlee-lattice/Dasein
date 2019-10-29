package com.latticeengines.apps.cdl.repository;

import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMessage;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;

public interface DataIntegrationStatusMonitoringRepository
        extends BaseJpaRepository<DataIntegrationStatusMonitor, Long> {

    DataIntegrationStatusMonitor findByWorkflowRequestId(String workflowRequestId);

    List<DataIntegrationStatusMonitor> findAllByTenantPid(Long tenantPid);

    List<DataIntegrationStatusMonitor> findAllByTenantPidAndEntityNameAndEntityIdIn(Long tenantPid, String entityName,
            List<String> entityIds);

    @Query("SELECT * from PLS_MultiTenant.DATA_INTEG_STATUS_MESSAGE msg where " +
            "FK_DATA_INTEG_MONITORING_ID = (SELECT PID from PLS_MultiTenant.DATA_INTEG_STATUS_MONITORING mon " +
            "where mon.ENTITY_ID like :launchid) " +
            "ORDER by msg.UPDATED_DATE DESC LIMIT 1")
    DataIntegrationStatusMessage findLatestMsg(@Param("launchid")String launchId);
}
