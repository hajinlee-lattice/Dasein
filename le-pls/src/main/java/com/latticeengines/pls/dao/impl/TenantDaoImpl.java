package com.latticeengines.pls.dao.impl;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.TenantDao;

public class TenantDaoImpl extends BaseDaoImpl<Tenant> implements TenantDao {

    @Override
    protected Class<Tenant> getEntityClass() {
        return Tenant.class;
    }

}
