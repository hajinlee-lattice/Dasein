package com.latticeengines.apps.cdl.repository;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.PlayGroup;

public interface PlayGroupRepository extends BaseJpaRepository<PlayGroup, Long> {
    PlayGroup findById(String id);

    PlayGroup findByPid(Long pid);
}
