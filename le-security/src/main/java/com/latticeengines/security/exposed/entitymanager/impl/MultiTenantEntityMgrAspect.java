package com.latticeengines.security.exposed.entitymanager.impl;

import org.aspectj.lang.JoinPoint;
import org.hibernate.SessionFactory;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.SecurityContextUtils;

public class MultiTenantEntityMgrAspect {

    public void enableMultiTenantFilter(JoinPoint joinPoint, SessionFactory sessionFactory,
            TenantEntityMgr tenantEntityMgr) {
        Tenant tenant = SecurityContextUtils.getTenant();
        sessionFactory.getCurrentSession().enableFilter("tenantFilter")
                .setParameter("tenantFilterId", tenant.getPid());
    }

}
