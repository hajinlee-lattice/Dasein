package com.latticeengines.pls.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.pls.entitymanager.ActionEntityMgr;
import com.latticeengines.pls.service.ActionService;

@Component("actionService")
public class ActionServiceImpl implements ActionService {

    @Inject
    private ActionEntityMgr actionEntityMgr;

    @Override
    public List<Action> findAll() {
        return actionEntityMgr.findAll();
    }

    @Override
    public List<Action> findByOwnerId(Long ownerId, Pageable pageable) {
        return actionEntityMgr.findByOwnerId(ownerId, pageable);
    }

    @Override
    public void delete(Action action) {
        actionEntityMgr.delete(action);
    }

    @Override
    public Action findByPid(Long pid) {
        return actionEntityMgr.findByPid(pid);
    }

    @Override
    public Action create(Action action) {
        actionEntityMgr.create(action);
        return action;
    }

    @Override
    public Action update(Action action) {
        actionEntityMgr.createOrUpdate(action);
        return action;
    }

    @Override
    public void updateOwnerIdIn(Long ownerId, List<Long> actionPids) {
        actionEntityMgr.updateOwnerIdIn(ownerId, actionPids);
    }

    @Override
    public List<Action> findByPidIn(List<Long> actionPids) {
        return actionEntityMgr.findByPidIn(actionPids);
    }

}
