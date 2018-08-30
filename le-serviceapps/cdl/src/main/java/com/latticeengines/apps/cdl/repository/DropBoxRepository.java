package com.latticeengines.apps.cdl.repository;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.DropBox;
import com.latticeengines.domain.exposed.security.Tenant;

public interface DropBoxRepository extends BaseJpaRepository<DropBox, Long> {

    DropBox findByTenant(Tenant tenant);

    boolean existsByTenant(Tenant tenant);

    boolean existsByDropBox(String dropBox);

}
