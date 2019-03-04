package com.latticeengines.db.exposed.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.security.Tenant;

public interface TenantEntityMgr extends BaseEntityMgrRepository<Tenant, Long> {

    Tenant findByTenantPid(Long tenantPid);

    Tenant findByTenantId(String tenantId);

    List<String> findAllTenantId();

    Tenant findByTenantName(String tenantName);

}
