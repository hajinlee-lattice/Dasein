package com.latticeengines.security.exposed.entitymanager.impl;

import org.aspectj.lang.JoinPoint;
import org.hibernate.SessionFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.TenantToken;
import com.latticeengines.security.exposed.TicketAuthenticationToken;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

public class MultiTenantEntityMgrAspect {

    public void enableMultiTenantFilter(JoinPoint joinPoint, SessionFactory sessionFactory,
            TenantEntityMgr tenantEntityMgr) {
        Tenant tenant = null;
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth instanceof TicketAuthenticationToken) {
            TicketAuthenticationToken token = (TicketAuthenticationToken) auth;
            tenant = token.getSession().getTenant();
        } else if (auth instanceof TenantToken) {
            tenant = ((TenantToken) auth).getTenant();
        } else {
            throw new RuntimeException("Problem with multi-tenancy framework.");
        }
        Tenant tenantWithPid = tenantEntityMgr.findByTenantId(tenant.getId());

        if (tenantWithPid == null) {
            throw new RuntimeException("No tenant found with id " + tenant.getId());
        }
        if (tenantWithPid.getPid() == null) {
            throw new RuntimeException("No tenant pid found for tenant with id " + tenant.getId());
        }
        tenant.setPid(tenantWithPid.getPid());
        sessionFactory.getCurrentSession().enableFilter("tenantFilter")
                .setParameter("tenantFilterId", tenantWithPid.getPid());
    }

}
