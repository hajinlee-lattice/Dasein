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
        if (action.getTenant() != null && tenant != null) {
            log.info(String.format("Action tenant: %s, updated to: %s", action.getTenant().getId(), tenant.getId()));
        }
        action.setTenant(tenant);
        actionEntityMgr.create(action);
        return action;
    }

    @Override
    public Action update(Action action) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (action.getTenant() != null && tenant != null) {
            log.info(String.format("Action tenant: %s, updated to: %s", action.getTenant().getId(), tenant.getId()));
        }
        action.setTenant(tenant);
        actionEntityMgr.createOrUpdate(action);
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

}
