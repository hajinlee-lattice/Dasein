package com.latticeengines.pls.entitymanager;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.pls.Action;

public interface ActionEntityMgr extends BaseEntityMgrRepository<Action, Long> {

    void updateOwnerIdIn(Long ownerId, List<Long> actionPids);

    Action findByPid(@NonNull Long pid);

    List<Action> findByOwnerId(@Nullable Long ownerId, Pageable pageable);

    List<Action> findByPidIn(List<Long> actionPids);

}
