package com.latticeengines.pls.repository;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.Action;

public interface ActionRepository extends BaseJpaRepository<Action, Long> {

    public Action findByPid(@NonNull Long pid);

    public List<Action> findByOwnerId(@Nullable Long ownerId);

    public List<Action> findByOwnerId(@Nullable Long ownerId, Pageable pageable);

}
