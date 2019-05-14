package com.latticeengines.apps.core.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.entitymgr.ActionEntityMgr;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionStatus;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("actionService")
public class ActionServiceImpl implements ActionService {

    private static final Logger log = LoggerFactory.getLogger(ActionServiceImpl.class);

    @Inject
    private ActionEntityMgr actionEntityMgr;

    @Override
    public List<Action> findAll() {
        return actionEntityMgr.findAll();
    }

    @Override
    public List<Action> findByOwnerId(Long ownerId) {
        return actionEntityMgr.findByOwnerId(ownerId, null);
    }

    @Override
    public void delete(Long actionPid) {
        if (actionPid != null) {
            Action action = findByPid(actionPid);
            if (action != null) {
                actionEntityMgr.delete(action);
            }
        }
    }

    @Override
    public Action findByPid(Long pid) {
        return actionEntityMgr.findByPid(pid);
    }

    @Override
    public Action create(Action action) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (action.getTenant() != null) {
            log.info(String.format("Action tenant: %s", action.getTenant().getId()));
        }
        if (tenant != null) {
            log.info(String.format("Action new tenant: %s", tenant.getId()));
        }
        action.setTenant(tenant);
        actionEntityMgr.create(action);
        log.info(String.format("Create Action=%s", action));
        return action;
    }

    @Override
    public List<Action> create(List<Action> actions) {
        Tenant tenant = MultiTenantContext.getTenant();
        actions.forEach(action -> action.setTenant(tenant));
        actionEntityMgr.create(actions);
        return actions;
    }

    @Override
    public List<Action> copy(List<Action> actions) {
        Tenant tenant = MultiTenantContext.getTenant();
        actions.forEach(action -> action.setTenant(tenant));
        actionEntityMgr.copy(actions);
        return actions;
    }

    @Override
    public Action update(Action action) {
        Tenant tenant = MultiTenantContext.getTenant();
        action.setTenant(tenant);
        actionEntityMgr.createOrUpdate(action);
        log.info(String.format("Update Action=%s", action));
        return action;
    }

    @Override
    public void patchOwnerIdByPids(Long ownerId, List<Long> actionPids) {
        actionEntityMgr.updateOwnerIdIn(ownerId, actionPids);
    }

    @Override
    public List<Action> findByPidIn(List<Long> actionPids) {
        return actionEntityMgr.findByPidIn(actionPids);
    }


    @Override
    public Action cancel(Long actionPid) {
        log.info("actionPid = " + actionPid);
        if (actionPid != null) {
            Action action = findByPid(actionPid);
            if (action != null && action.getOwnerId() == null && ActionType.CDL_DATAFEED_IMPORT_WORKFLOW == action.getType()) {
                actionEntityMgr.cancel(actionPid);
            } else {
                String errMessage = "";
                if (action == null)
                    errMessage = "Can not find this action";
                else if (action.getOwnerId() != null)
                    errMessage = "This Action is running, can not be canceled.";
                else if (ActionType.CDL_DATAFEED_IMPORT_WORKFLOW != action.getType())
                    errMessage = "This Action is not import action, can not be canceled.";
                log.error(errMessage);
                throw new RuntimeException(errMessage);
            }
            action = findByPid(actionPid);
            return action;
        }
        return null;
    }

    @Override
    public List<Action> getActionsByJobPids(List<Long> jobPids) {
        return actionEntityMgr.getActionsByJobPids(jobPids);
    }

    @Override
    public List<Action> findByOwnerIdAndActionStatus(Long ownerId, ActionStatus actionStatus) {
        return actionEntityMgr.findByOwnerIdAndActionStatus(ownerId, actionStatus);
    }

}
