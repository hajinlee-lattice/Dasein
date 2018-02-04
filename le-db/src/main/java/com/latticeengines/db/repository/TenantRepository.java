package com.latticeengines.db.repository;

import org.springframework.data.jpa.repository.Query;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.security.Tenant;

public interface TenantRepository extends BaseJpaRepository<Tenant, Long> {

    @Query("select t from Tenant t where t.id = ?1")
    Tenant findByTenantId(String tenantId);

    Tenant findByName(String tenantName);

}
