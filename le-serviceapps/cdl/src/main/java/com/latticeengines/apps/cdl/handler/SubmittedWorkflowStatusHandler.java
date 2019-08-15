package com.latticeengines.apps.cdl.handler;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger log = LoggerFactory.getLogger(SubmittedWorkflowStatusHandler.class);

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
        
        List<DataIntegrationStatusMonitor> statusMonitors = dataIntegrationStatusMonitoringEntityMgr
                .getAllStatusesByEntityNameAndIds(tenant.getPid(), "PlayLaunch", Arrays.asList(status.getEntityId()));

        if (statusMonitors.size() > 0) {
            log.error(String.format("%s integration status monitors already exists with launch id %s",
                    statusMonitors.size(), status.getEntityId()));
        }

        statusMonitor = new DataIntegrationStatusMonitor(status, tenant);
        statusMonitor.setEventSubmittedTime(status.getEventTime());
        dataIntegrationStatusMonitoringEntityMgr.createStatus(statusMonitor);
        return statusMonitor;
    }

}
