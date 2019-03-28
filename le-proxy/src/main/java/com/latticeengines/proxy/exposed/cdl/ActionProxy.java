package com.latticeengines.proxy.exposed.cdl;

import java.util.List;

import com.latticeengines.domain.exposed.pls.Action;

public interface ActionProxy {

    Action createAction(String customerSpace, Action action);

    Action updateAction(String customerSpace, Action action);

    Action cancelAction(String customerSpace, Long actionPid);

    List<Action> getActions(String customerSpace);

    List<Action> getActionsByOwnerId(String customerSpace, Long ownerId);

    List<Action> getActionsByPids(String customerSpace, List<Long> actionPids);

    void patchOwnerIdByPids(String customerSpace, Long ownerId, List<Long> actionPids);

    Action getActionByJobPid(String customerSpace, Long jobPid);

}
