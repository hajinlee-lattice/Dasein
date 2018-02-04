package com.latticeengines.db.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.TenantDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("tenantDao")
public class TenantDaoImpl extends BaseDaoImpl<Tenant> implements TenantDao {

    @Override
    protected Class<Tenant> getEntityClass() {
        return Tenant.class;
    }

}
