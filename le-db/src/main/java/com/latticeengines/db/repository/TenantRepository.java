package com.latticeengines.db.repository;

import java.util.List;

import org.springframework.data.jpa.repository.Query;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.security.TenantType;

public interface TenantRepository extends BaseJpaRepository<Tenant, Long> {

    @Query("select t from Tenant t where t.id = ?1")
    Tenant findByTenantId(String tenantId);

    @Query("select t.id from Tenant t")
    List<String> findAllTenantId();

    @Query("select t.pid from Tenant t")
    List<Long> findAllTenantPid();

    Tenant findByName(String tenantName);

    Tenant findBySubscriberNumber(String subscriberNumber);

    List<Tenant> findAllByStatus(TenantStatus status);

    List<Tenant> findAllByTenantTypeIn(List<TenantType> types);

    List<Tenant> findByNameStartingWith(String tenantName);

}
