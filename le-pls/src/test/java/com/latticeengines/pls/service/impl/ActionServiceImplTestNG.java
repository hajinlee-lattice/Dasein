package com.latticeengines.pls.service.impl;

import java.util.List;

import javax.inject.Inject;

import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.pls.entitymanager.impl.ActionEntityMgrImplTestNG;
import com.latticeengines.pls.service.ActionService;

public class ActionServiceImplTestNG extends ActionEntityMgrImplTestNG {

    @Inject
    private ActionService actionService;

    @Override
    protected void createAction(Action action) {
        actionService.create(action);
    }

    @Override
    protected List<Action> findAll() {
        return actionService.findAll();
    }

    @Override
    protected List<Action> findByOwnerId(Long ownerId) {
        return actionService.findByOwnerId(ownerId, null);
    }

    @Override
    protected Action findByPid(Long pid) {
        return actionService.findByPid(pid);
    }

    @Override
    protected void update(Action action) {
        actionService.update(action);
    }

    @Override
    protected void delete(Action action) {
        actionService.delete(action);
    }

    @Override
    protected void updateOwnerIdIn(Long ownerId, List<Long> actionPids) {
        actionService.updateOwnerIdIn(ownerId, actionPids);
    }

    @Override
    protected List<Action> findByPidIn(List<Long> actionPids) {
        return actionService.findByPidIn(actionPids);
    }

}
