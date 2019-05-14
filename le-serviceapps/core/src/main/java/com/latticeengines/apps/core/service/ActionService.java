package com.latticeengines.apps.core.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionStatus;

public interface ActionService {

    Action create(Action action);

    List<Action> create(List<Action> actions);

    List<Action> copy(List<Action> actions);

    Action update(Action action);

    List<Action> findAll();

    List<Action> findByOwnerId(Long ownerId);

    void patchOwnerIdByPids(Long ownerId, List<Long> actionPids);

    void delete(Long actionPid);

    Action findByPid(Long pid);

    List<Action> findByPidIn(List<Long> actionPids);

    Action cancel(Long actionPid);

    List<Action> getActionsByJobPids(List<Long> jobPids);

    List<Action> findByOwnerIdAndActionStatus(Long ownerId, ActionStatus actionStatus);
}
