package com.latticeengines.apps.core.service.impl;

import java.util.List;

import javax.inject.Inject;

import com.latticeengines.apps.core.entitymgr.impl.ActionEntityMgrImplTestNG;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.domain.exposed.pls.Action;

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
        return actionService.findByOwnerId(ownerId);
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
        actionService.delete(action.getPid());
    }

    @Override
    protected void updateOwnerIdIn(Long ownerId, List<Long> actionPids) {
        actionService.patchOwnerIdByPids(ownerId, actionPids);
    }

    @Override
    protected List<Action> findByPidIn(List<Long> actionPids) {
        return actionService.findByPidIn(actionPids);
    }

    @Override
    protected  List<Action> getActionsByJobPid(Long jobPid) { return actionService.getActionsByJobPid(jobPid); }

}
