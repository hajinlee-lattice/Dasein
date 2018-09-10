package com.latticeengines.apps.cdl.repository;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.PlayType;

public interface PlayTypeRepository extends BaseJpaRepository<PlayType, Long> {
    PlayType findById(String id);

    PlayType findByPid(Long pid);
}
