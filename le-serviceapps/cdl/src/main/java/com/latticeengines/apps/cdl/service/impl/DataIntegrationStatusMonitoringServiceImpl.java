package com.latticeengines.apps.cdl.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMessageEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMonitoringEntityMgr;
import com.latticeengines.apps.cdl.service.DataIntegrationStatusMonitoringService;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMessage;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.MessageType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

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
        log.info("Creating/updating status with monitoring message " + JsonUtils.serialize(status));
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
        log.info("Creating new status message: " + JsonUtils.serialize(status));
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
    public List<DataIntegrationStatusMonitor> getAllStatuses(String tenantId) {
        Tenant tenant = tenantEntityMgr.findByTenantId(tenantId);
        return dataIntegrationStatusMonitoringEntityMgr.getAllStatuses(tenant.getPid());
    }

    private WorkflowStatusHandler getWorkflowStatusHandler(String type) {
        DataIntegrationEventType eventType = DataIntegrationEventType.valueOf(type);
        WorkflowStatusHandler workflowStatusHandler = null;
        if (eventType != null) {
            workflowStatusHandler = eventHandlerMap.get(eventType);
        }
        if (workflowStatusHandler == null) {
            throw new LedpException(LedpCode.LEDP_40048, new String[] { type });
        }
        log.info("WorkflowStatusHandler for type " + type);
        return workflowStatusHandler;
    }

    public interface WorkflowStatusHandler {
        public DataIntegrationStatusMonitor handleWorkflowState(
                DataIntegrationStatusMonitor statusMonitor,
                DataIntegrationStatusMonitorMessage status);

        public DataIntegrationEventType getEventType();

        public default void checkStatusMonitorExists(DataIntegrationStatusMonitor statusMonitor,
                DataIntegrationStatusMonitorMessage status) {
            if (statusMonitor == null) {
                throw new LedpException(LedpCode.LEDP_40047, new String[] { "null", status.getEventType() });
            }
        }

        public default void updateMonitoringStatus(DataIntegrationStatusMonitor statusMonitor,
                String messageEventType) {
            if (DataIntegrationEventType.canTransit(DataIntegrationEventType.valueOf(statusMonitor.getStatus()),
                    DataIntegrationEventType.valueOf(messageEventType))) {
                statusMonitor.setStatus(messageEventType);
            }
        }

    }

    @Component
    public class SubmittedWorkflowStatusHandler implements WorkflowStatusHandler {

        SubmittedWorkflowStatusHandler() {
            eventHandlerMap.put(DataIntegrationEventType.WORKFLOW_SUBMITTED, this);
        }

        @Override
        public DataIntegrationEventType getEventType() {
            return DataIntegrationEventType.WORKFLOW_SUBMITTED;
        }

        @Override
        public DataIntegrationStatusMonitor handleWorkflowState(
                DataIntegrationStatusMonitor statusMonitor,
                DataIntegrationStatusMonitorMessage status) {
            Tenant tenant = tenantEntityMgr.findByTenantName(status.getTenantId());

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

    @Component
    public class StartedWorkflowStatusHandler implements WorkflowStatusHandler {

        StartedWorkflowStatusHandler() {
            eventHandlerMap.put(DataIntegrationEventType.WORKFLOW_STARTED, this);
        }

        @Override
        public DataIntegrationEventType getEventType() {
            return DataIntegrationEventType.WORKFLOW_STARTED;
        }

        @Override
        public DataIntegrationStatusMonitor handleWorkflowState(DataIntegrationStatusMonitor statusMonitor,
                DataIntegrationStatusMonitorMessage status) {

            checkStatusMonitorExists(statusMonitor, status);

            statusMonitor.setEventStartedTime(status.getEventTime());

            updateMonitoringStatus(statusMonitor, status.getEventType());

            return dataIntegrationStatusMonitoringEntityMgr.updateStatus(statusMonitor);
        }

    }

    @Component
    public class CompletedWorkflowStatusHandler implements WorkflowStatusHandler {

        CompletedWorkflowStatusHandler() {
            eventHandlerMap.put(DataIntegrationEventType.WORKFLOW_COMPLETED, this);
        }

        @Override
        public DataIntegrationEventType getEventType() {
            return DataIntegrationEventType.WORKFLOW_COMPLETED;
        }

        @Override
        public DataIntegrationStatusMonitor handleWorkflowState(DataIntegrationStatusMonitor statusMonitor,
                DataIntegrationStatusMonitorMessage status) {

            checkStatusMonitorExists(statusMonitor, status);

            statusMonitor.setEventCompletedTime(status.getEventTime());

            updateMonitoringStatus(statusMonitor, status.getEventType());

            return dataIntegrationStatusMonitoringEntityMgr.updateStatus(statusMonitor);
        }

    }

    @Component
    public class FailedWorkflowStatusHandler implements WorkflowStatusHandler {

        FailedWorkflowStatusHandler() {
            eventHandlerMap.put(DataIntegrationEventType.WORKFLOW_FAILED, this);
        }

        @Override
        public DataIntegrationEventType getEventType() {
            return DataIntegrationEventType.WORKFLOW_FAILED;
        }

        @Override
        public DataIntegrationStatusMonitor handleWorkflowState(DataIntegrationStatusMonitor statusMonitor,
                DataIntegrationStatusMonitorMessage status) {

            checkStatusMonitorExists(statusMonitor, status);

            statusMonitor.setStatus(status.getEventType());
            statusMonitor.setErrorFile(status.getErrorFile());

            return dataIntegrationStatusMonitoringEntityMgr.updateStatus(statusMonitor);
        }

    }

}
