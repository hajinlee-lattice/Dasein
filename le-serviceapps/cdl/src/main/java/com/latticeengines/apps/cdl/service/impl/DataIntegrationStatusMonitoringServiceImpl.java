package com.latticeengines.apps.cdl.service.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMonitoringEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMessageEntityMgr;
import com.latticeengines.apps.cdl.service.DataIntegrationStatusMonitoringService;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMessage;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.MessageType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("dataIntegrationStatusMonitoringService")
public class DataIntegrationStatusMonitoringServiceImpl
        implements DataIntegrationStatusMonitoringService {

    private static final Logger log = LoggerFactory
            .getLogger(DataIntegrationStatusMonitoringServiceImpl.class);

    private Map<DataIntegrationEventType, WorkflowStatusHandler> eventHandlerMap;

    @PostConstruct
    public void postConstruct() {
        eventHandlerMap = new HashMap<>();
        new SubmittedWorkflowStatusHandler();
        new StartedWorkflowStatusHandler();
        new CompletedWorkflowStatusHandler();
        new FailedWorkflowStatusHandler();
    }
    //
    // @Autowired
    // private List<WorkflowStatusHandler> handlerList;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private DataIntegrationStatusMonitoringEntityMgr dataIntegrationStatusMonitoringEntityMgr;

    @Inject
    private DataIntegrationStatusMessageEntityMgr dataIntegrationStatusMsgEntityMgr;

    @Override
    public boolean createOrUpdateStatus(DataIntegrationStatusMonitorMessage status) {
        DataIntegrationStatusMonitor statusMonitor = handleStatusMonitor(status);
        createNewStatusMessage(status, statusMonitor);
        return true;
    }

    private DataIntegrationStatusMonitor handleStatusMonitor(
            DataIntegrationStatusMonitorMessage status) {
        DataIntegrationStatusMonitor statusMonitor = dataIntegrationStatusMonitoringEntityMgr
                .getStatus(status.getWorkflowRequestId());
        WorkflowStatusHandler handler = getWorkflowStatusHandler(status.getEventType());
        return handler.handleWorkflowState(statusMonitor, status);
    }

    private void createNewStatusMessage(DataIntegrationStatusMonitorMessage status,
            DataIntegrationStatusMonitor statusMonitor) {
        DataIntegrationStatusMessage statusMessage = new DataIntegrationStatusMessage();
        statusMessage.setStatusMonitor(statusMonitor);
        statusMessage.setMessageType(MessageType.valueOf(status.getMessageType().toUpperCase()));
        statusMessage.setMessage(status.getMessage());
        statusMessage.setEventType(status.getEventType());
        statusMessage.setEventTime(status.getEventTime());
        dataIntegrationStatusMsgEntityMgr.create(statusMessage);
    }

    @Override
    public DataIntegrationStatusMonitor getStatus(String eventId) {
        return dataIntegrationStatusMonitoringEntityMgr.getStatus(eventId);
    }

    @Override
    public List<DataIntegrationStatusMonitor> getAllStatuses(Long tenantId) {
        return dataIntegrationStatusMonitoringEntityMgr.getAllStatuses(tenantId);
    }

    private WorkflowStatusHandler getWorkflowStatusHandler(String type) {
        DataIntegrationEventType eventType = DataIntegrationEventType.valueOf(type);
        WorkflowStatusHandler workflowStatusHandler = null;
        if (eventType != null) {
            workflowStatusHandler = eventHandlerMap.get(eventType);
        }
        if (workflowStatusHandler == null) {
            throw new LedpException(LedpCode.LEDP_00000);
        }
        return workflowStatusHandler;
    }

    protected void updateStatus(DataIntegrationStatusMonitor statusMonitor, String type,
            Date time) {
        DataIntegrationEventType eventType = DataIntegrationEventType.valueOf(type);
        switch (eventType) {
        case WORKFLOW_SUBMITTED:
            statusMonitor.setEventSubmittedTime(time);
            break;
        case WORKFLOW_STARTED:
            statusMonitor.setEventStartedTime(time);
            break;
        case WORKFLOW_COMPLETED:
            statusMonitor.setEventCompletedTime(time);
            break;
        }
        statusMonitor.setStatus(type);
    }

    public interface WorkflowStatusHandler {
        public DataIntegrationStatusMonitor handleWorkflowState(
                DataIntegrationStatusMonitor statusMonitor,
                DataIntegrationStatusMonitorMessage status);

    }

    @Component
    public class SubmittedWorkflowStatusHandler implements WorkflowStatusHandler {

        SubmittedWorkflowStatusHandler() {
            eventHandlerMap.put(DataIntegrationEventType.WORKFLOW_SUBMITTED, this);
        }

        @Override
        public DataIntegrationStatusMonitor handleWorkflowState(
                DataIntegrationStatusMonitor statusMonitor,
                DataIntegrationStatusMonitorMessage status) {
            if (statusMonitor != null) {
                throw new LedpException(LedpCode.LEDP_00000);
            }

            statusMonitor = new DataIntegrationStatusMonitor();
            Tenant tenant = tenantEntityMgr.findByTenantPid(status.getTenantId());
            statusMonitor.setTenant(tenant);
            statusMonitor.setWorkflowRequestId(status.getWorkflowRequestId());
            statusMonitor.setOperation(status.getOperation());
            statusMonitor.setEntityId(status.getEntityId());
            statusMonitor.setEntityName(status.getEntityName());
            statusMonitor.setExternalSystemId(status.getExternalSystemId());
            statusMonitor.setEventSubmittedTime(status.getEventTime());
            statusMonitor.setStatus(DataIntegrationEventType.WORKFLOW_SUBMITTED.toString());
            statusMonitor.setSourceFile(status.getSourceFile());
            dataIntegrationStatusMonitoringEntityMgr.createStatus(statusMonitor);
            return statusMonitor;
        }

    }

    @Component
    public class StartedWorkflowStatusHandler implements WorkflowStatusHandler {

        StartedWorkflowStatusHandler() {
            eventHandlerMap.put(DataIntegrationEventType.WORKFLOW_STARTED, this);
        }

        @Override
        public DataIntegrationStatusMonitor handleWorkflowState(
                DataIntegrationStatusMonitor statusMonitor,
                DataIntegrationStatusMonitorMessage status) {
            if (!DataIntegrationEventType.canTransit(
                    DataIntegrationEventType.valueOf(statusMonitor.getStatus()),
                    DataIntegrationEventType.valueOf(status.getEventType()))) {
                updateStatus(statusMonitor, status.getEventType(), status.getEventTime());
            }

            statusMonitor.setEventStartedTime(status.getEventTime());

            if (status.getErrorFile() != null) {
                statusMonitor.setErrorFile(status.getErrorFile());
            }

            return dataIntegrationStatusMonitoringEntityMgr.updateStatus(statusMonitor);
        }

    }

    @Component
    public class CompletedWorkflowStatusHandler implements WorkflowStatusHandler {

        CompletedWorkflowStatusHandler() {
            eventHandlerMap.put(DataIntegrationEventType.WORKFLOW_COMPLETED, this);
        }

        @Override
        public DataIntegrationStatusMonitor handleWorkflowState(
                DataIntegrationStatusMonitor statusMonitor,
                DataIntegrationStatusMonitorMessage status) {
            if (!DataIntegrationEventType.canTransit(
                    DataIntegrationEventType.valueOf(statusMonitor.getStatus()),
                    DataIntegrationEventType.valueOf(status.getEventType()))) {
                updateStatus(statusMonitor, status.getEventType(), status.getEventTime());
            }

            statusMonitor.setEventCompletedTime(status.getEventTime());

            if (status.getErrorFile() != null) {
                statusMonitor.setErrorFile(status.getErrorFile());
            }

            return dataIntegrationStatusMonitoringEntityMgr.updateStatus(statusMonitor);
        }

    }

    @Component
    public class FailedWorkflowStatusHandler implements WorkflowStatusHandler {

        FailedWorkflowStatusHandler() {
            eventHandlerMap.put(DataIntegrationEventType.WORKFLOW_FAILED, this);
        }

        @Override
        public DataIntegrationStatusMonitor handleWorkflowState(
                DataIntegrationStatusMonitor statusMonitor,
                DataIntegrationStatusMonitorMessage status) {
            if (!DataIntegrationEventType.canTransit(
                    DataIntegrationEventType.valueOf(statusMonitor.getStatus()),
                    DataIntegrationEventType.valueOf(status.getEventType()))) {
                updateStatus(statusMonitor, status.getEventType(), status.getEventTime());
            }

            if (status.getErrorFile() != null) {
                statusMonitor.setErrorFile(status.getErrorFile());
            }

            return dataIntegrationStatusMonitoringEntityMgr.updateStatus(statusMonitor);
        }

    }

}
