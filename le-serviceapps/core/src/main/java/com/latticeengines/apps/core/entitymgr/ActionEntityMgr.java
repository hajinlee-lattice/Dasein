package com.latticeengines.apps.core.entitymgr;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionStatus;

public interface ActionEntityMgr extends BaseEntityMgrRepository<Action, Long> {

    void create(List<Action> actions);

    void copy(List<Action> actions);

    void updateOwnerIdIn(Long ownerId, List<Long> actionPids);

    Action findByPid(@NonNull Long pid);

    List<Action> findByOwnerId(@Nullable Long ownerId, Pageable pageable);

    List<Action> findByPidIn(List<Long> actionPids);

    void cancel(Long actionPid);

    List<Action> getActionsByJobPids(List<Long> jobPid);

    List<Action> findByOwnerIdAndActionStatus(Long ownerId, ActionStatus actionStatus);
}
