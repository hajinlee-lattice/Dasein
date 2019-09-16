package com.latticeengines.apps.cdl.repository;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;

public interface S3ImportSystemRepository extends BaseJpaRepository<S3ImportSystem, Long> {

    S3ImportSystem findByName(String name);

    List<S3ImportSystem> findByMapToLatticeAccount(Boolean mapToLatticeAccount);

    List<S3ImportSystem> findByMapToLatticeContact(Boolean mapToLatticeContact);
}
