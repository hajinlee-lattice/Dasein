package com.latticeengines.security.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.security.GlobalAuthTenant;
import com.latticeengines.security.dao.GlobalAuthTenantDao;

@Component("globalAuthTenantDao")
public class GlobalAuthTenantDaoImpl extends BaseDaoImpl<GlobalAuthTenant> implements
        GlobalAuthTenantDao {

    @Override
    protected Class<GlobalAuthTenant> getEntityClass() {
        return GlobalAuthTenant.class;
    }

}
