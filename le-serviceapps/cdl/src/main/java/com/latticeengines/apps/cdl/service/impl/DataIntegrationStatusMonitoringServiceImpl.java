package com.latticeengines.apps.cdl.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMessageEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMonitoringEntityMgr;
import com.latticeengines.apps.cdl.handler.WorkflowStatusHandler;
import com.latticeengines.apps.cdl.service.DataIntegrationStatusMonitoringService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMessage;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.MessageType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("dataIntegrationStatusMonitoringService")
public class DataIntegrationStatusMonitoringServiceImpl implements DataIntegrationStatusMonitoringService {

    private static final Logger log = LoggerFactory.getLogger(DataIntegrationStatusMonitoringServiceImpl.class);

    private Map<DataIntegrationEventType, WorkflowStatusHandler> eventHandlerMap;


    @PostConstruct
    public void postConstruct() {
        eventHandlerMap = new HashMap<>();
        for (WorkflowStatusHandler handler : handlerList) {
            eventHandlerMap.put(handler.getEventType(), handler);
        }
        log.info("eventHandlerMap postConstruct: " + JsonUtils.serialize(eventHandlerMap));
    }

    @Autowired
    private List<WorkflowStatusHandler> handlerList;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private DataIntegrationStatusMonitoringEntityMgr dataIntegrationStatusMonitoringEntityMgr;

    @Inject
    private DataIntegrationStatusMessageEntityMgr dataIntegrationStatusMsgEntityMgr;

    @Override
    public Map<String, Boolean> createOrUpdateStatuses(List<DataIntegrationStatusMonitorMessage> statuses) {
        log.info("Creating/updating statuses with monitoring message ");
        Map<String, Boolean> statusesUpdate = new HashMap<>();
        statuses.forEach(status -> {
            DataIntegrationStatusMonitor statusMonitor = handleStatusMonitor(status);
            statusesUpdate.put(status.getWorkflowRequestId(), createNewStatusMessage(status, statusMonitor));
        });

        return statusesUpdate;
    }

    private DataIntegrationStatusMonitor handleStatusMonitor(DataIntegrationStatusMonitorMessage status) {
        DataIntegrationStatusMonitor statusMonitor = dataIntegrationStatusMonitoringEntityMgr
                .getStatus(status.getWorkflowRequestId());
        if (statusMonitor != null && statusMonitor.getTenant() != null) {
            MultiTenantContext.setTenant(statusMonitor.getTenant());
        }
        WorkflowStatusHandler handler = getWorkflowStatusHandler(status.getEventType());
        return handler.handleWorkflowState(statusMonitor, status);
    }

    private boolean createNewStatusMessage(DataIntegrationStatusMonitorMessage status,
            DataIntegrationStatusMonitor statusMonitor) {
        log.info("Creating new status message: " + JsonUtils.serialize(status));
        try {
            DataIntegrationStatusMessage statusMessage = new DataIntegrationStatusMessage();
            statusMessage.setTenant(statusMonitor.getTenant());
            statusMessage.setStatusMonitor(statusMonitor);
            statusMessage.setMessageType(MessageType.valueOf(status.getMessageType()));
            statusMessage.setMessage(status.getMessage());
            statusMessage.setEventType(status.getEventType());
            statusMessage.setEventTime(status.getEventTime());
            statusMessage.setEventDetail(status.getEventDetail());
            dataIntegrationStatusMsgEntityMgr.create(statusMessage);
            return true;
        } catch (Exception ex) {
            log.warn("Creation new status message " + ex.getMessage());
            return false;
        }
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

    @Override
    public List<DataIntegrationStatusMonitor> getAllStatusesByEntityNameAndIds(String tenantId, String entityName,
            List<String> entityIds) {
        Tenant tenant = tenantEntityMgr.findByTenantId(tenantId);
        return dataIntegrationStatusMonitoringEntityMgr.getAllStatusesByEntityNameAndIds(tenant.getPid(), entityName,
                entityIds);
    }

    @Override
    public List<DataIntegrationStatusMessage> getAllStatusMessages(String workflowRequestId) {
        DataIntegrationStatusMonitor statusMonitor = dataIntegrationStatusMonitoringEntityMgr
                .getStatus(workflowRequestId);
        return dataIntegrationStatusMsgEntityMgr.getAllStatusMessages(statusMonitor.getPid());
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

}
