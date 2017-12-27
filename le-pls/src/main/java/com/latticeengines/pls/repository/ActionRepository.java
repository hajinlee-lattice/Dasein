package com.latticeengines.pls.repository;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.Action;

public interface ActionRepository extends BaseJpaRepository<Action, Long> {

    @Modifying
    @Transactional(readOnly = false)
    @Query("update Action a set a.ownerId = ?1 where a.pid in ?2")
    void updateOwnerIdIn(Long ownerId, List<Long> actionPids);

    public Action findByPid(@NonNull Long pid);

    public List<Action> findByOwnerId(@Nullable Long ownerId);

    public List<Action> findByOwnerId(@Nullable Long ownerId, Pageable pageable);

    public List<Action> findByPidIn(List<Long> actionPids);

}
