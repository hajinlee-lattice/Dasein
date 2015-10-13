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
        if (tenant == null) {
            throw new RuntimeException("Problem with multi-tenancy framework");
        }

        Tenant tenantWithPid = tenantEntityMgr.findByTenantId(tenant.getId());

        if (tenantWithPid == null) {
            throw new RuntimeException("No tenant found with id " + tenant.getId());
        }
        if (tenantWithPid.getPid() == null) {
            throw new RuntimeException("No tenant pid found for tenant with id " + tenant.getId());
        }
        tenant.setPid(tenantWithPid.getPid());

        if (tenantWithPid.getPid() == null) {
            throw new RuntimeException("Problem with multi-tenancy framework");
        }

        sessionFactory.getCurrentSession().enableFilter("tenantFilter")
                .setParameter("tenantFilterId", tenantWithPid.getPid());

    }

}
