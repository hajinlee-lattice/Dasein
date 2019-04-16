package com.latticeengines.apps.cdl.repository;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;

public interface S3ImportSystemRepository extends BaseJpaRepository<S3ImportSystem, Long> {

    S3ImportSystem findByName(String name);
}
