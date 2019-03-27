package com.latticeengines.apps.cdl.handler;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMonitoringEntityMgr;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;

@Component
public class SubmittedWorkflowStatusHandler implements WorkflowStatusHandler {

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private DataIntegrationStatusMonitoringEntityMgr dataIntegrationStatusMonitoringEntityMgr;

    @Override
    public DataIntegrationEventType getEventType() {
        return DataIntegrationEventType.WorkflowSubmitted;
    }

    @Override
    public DataIntegrationStatusMonitor handleWorkflowState(DataIntegrationStatusMonitor statusMonitor,
            DataIntegrationStatusMonitorMessage status) {
        Tenant tenant = tenantEntityMgr.findByTenantName(status.getTenantName());

        if (statusMonitor != null) {
            throw new LedpException(LedpCode.LEDP_40047,
                    new String[] { statusMonitor.getStatus(), status.getEventType() });
        }

        statusMonitor = new DataIntegrationStatusMonitor(status, tenant);
        statusMonitor.setEventSubmittedTime(status.getEventTime());
        dataIntegrationStatusMonitoringEntityMgr.createStatus(statusMonitor);
        return statusMonitor;
    }

}
