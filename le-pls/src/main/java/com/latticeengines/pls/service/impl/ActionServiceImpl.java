package com.latticeengines.pls.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.pls.service.ActionService;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;

@Component("actionService")
public class ActionServiceImpl implements ActionService {

    @Inject
    private ActionProxy actionProxy;

    @Override
    public List<Action> findAll() {
        String tenantId = MultiTenantContext.getTenantId();
        return actionProxy.getActions(tenantId);
    }

    @Override
    public List<Action> findByOwnerId(Long ownerId, Pageable pageable) {
        String tenantId = MultiTenantContext.getTenantId();
        return actionProxy.getActionsByOwnerId(tenantId, ownerId);
    }

    @Override
    public Action create(Action action) {
        String tenantId = MultiTenantContext.getTenantId();
        return actionProxy.createAction(tenantId, action);
    }

    @Override
    public Action update(Action action) {
        String tenantId = MultiTenantContext.getTenantId();
        return actionProxy.updateAction(tenantId, action);
    }

    @Override
    public List<Action> findByPidIn(List<Long> actionPids) {
        String tenantId = MultiTenantContext.getTenantId();
        return actionProxy.getActionsByPids(tenantId, actionPids);
    }

}
