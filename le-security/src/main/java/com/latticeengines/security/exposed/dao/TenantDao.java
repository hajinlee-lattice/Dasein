package com.latticeengines.security.exposed.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.security.Tenant;

public interface TenantDao extends BaseDao<Tenant> {

    Tenant findByTenantId(String tenantId);

    Tenant findByTenantName(String tenantName);
}
