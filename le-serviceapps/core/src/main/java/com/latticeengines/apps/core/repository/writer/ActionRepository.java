package com.latticeengines.apps.core.repository.writer;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.Action;

public interface ActionRepository extends BaseJpaRepository<Action, Long> {

    Action findByPid(@NonNull Long pid);

    List<Action> findByOwnerId(@Nullable Long ownerId);

    List<Action> findByOwnerId(@Nullable Long ownerId, Pageable pageable);

    List<Action> findByPidIn(List<Long> actionPids);

    List<Action> findAllByTrackingPid(@NonNull Long trackingPid);
}
