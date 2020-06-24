package com.latticeengines.apps.core.repository.writer;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionStatus;
import com.latticeengines.domain.exposed.pls.ActionType;

public interface ActionRepository extends BaseJpaRepository<Action, Long> {

    Action findByPid(@NonNull Long pid);

    List<Action> findByOwnerId(@Nullable Long ownerId);

    List<Action> findByOwnerId(@Nullable Long ownerId, Pageable pageable);

    List<Action> findByPidIn(List<Long> actionPids);

    List<Action> findAllByTrackingPidIn(@NonNull List<Long> trackingPid);

    List<Action> findByOwnerIdAndActionStatus(@NonNull Long ownerId, ActionStatus actionStatus);

    @Query("SELECT a.pid FROM Action a JOIN WorkflowJob wj ON a.ownerId = wj.pid " +
            "WHERE a.type = :actionType AND wj.type = :workflowType AND a.actionConfiguration LIKE %:partialConfig%")
    List<Object[]> findActionPidByActionTypeAndOwnerWorkflowType(@Param("actionType") ActionType actionType,
                                                                 @Param("workflowType") String workflowType,
                                                                 @Param("partialConfig") String partialConfig);

    @Query("SELECT a.pid FROM Action a WHERE a.ownerId IS NULL AND a.type = :actionType " +
            "AND a.actionStatus = :actionStatus AND a.actionConfiguration LIKE %:partialConfig%")
    List<Object[]> findActionPidWithoutOwnerByTypeAndStatus(@Param("actionType") ActionType actionType,
                                                            @Param("actionStatus") ActionStatus actionStatus,
                                                            @Param("partialConfig") String partialConfig);
}
