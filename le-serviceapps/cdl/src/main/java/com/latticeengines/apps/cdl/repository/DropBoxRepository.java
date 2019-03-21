package com.latticeengines.apps.cdl.repository;

import org.springframework.data.jpa.repository.Query;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.DropBox;
import com.latticeengines.domain.exposed.security.Tenant;

public interface DropBoxRepository extends BaseJpaRepository<DropBox, Long> {

    DropBox findByTenant(Tenant tenant);

    boolean existsByTenant(Tenant tenant);

    boolean existsByDropBox(String dropBox);

    @Query("SELECT tenant from DropBox where dropBox = ?1")
    Tenant findTenantByDropBox(String dropBox);

    DropBox findByDropBox(String dropBox);

}
