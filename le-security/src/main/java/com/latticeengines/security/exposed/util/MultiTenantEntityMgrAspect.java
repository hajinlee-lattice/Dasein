package com.latticeengines.security.exposed.util;

import javax.persistence.EntityManager;

import org.aspectj.lang.JoinPoint;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.hibernate.Filter;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;

public class MultiTenantEntityMgrAspect {
    private static final Logger log = LoggerFactory.getLogger(MultiTenantEntityMgrAspect.class);
    
    public void enableMultiTenantFilter(JoinPoint joinPoint, SessionFactory sessionFactory,
            TenantEntityMgr tenantEntityMgr) {
        
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr, null);
    }
    
    public void enableMultiTenantFilter(JoinPoint joinPoint, SessionFactory sessionFactory,
            TenantEntityMgr tenantEntityMgr, EntityManager entityManager) {
        
        Tenant tenant = MultiTenantContext.getTenant();
        if (tenant == null) {
            throw new RuntimeException("Problem with multi-tenancy framework");
        }

        if (tenant.getPid() == null) {
            // Technically, control should never come here, because Tenant.pid should never be null. Just to reduce the risk and make the backward compatibility leaving this code.
            log.warn("Tenant PID is null. Need to fix the code path", new RuntimeException("Check the stacktrace for PID is null"));
            
            Tenant tenantWithPid = tenantEntityMgr.findByTenantId(tenant.getId());

            if (tenantWithPid == null) {
                throw new RuntimeException("No tenant found with id " + tenant.getId());
            }
            if (tenantWithPid.getPid() == null) {
                throw new RuntimeException("No tenant pid found for tenant with id " + tenant.getId());
            }
            tenant.setPid(tenantWithPid.getPid());
        }

        if (tenant.getPid() == null) {
            throw new RuntimeException("Problem with multi-tenancy framework");
        }

        sessionFactory.getCurrentSession().enableFilter("tenantFilter")
                .setParameter("tenantFilterId", tenant.getPid());
        
        if (entityManager != null) {
            Filter filter = (Filter)entityManager.unwrap(Session.class).enableFilter("tenantFilter");
            filter.setParameter("tenantFilterId", tenant.getPid());
        }
        
    }

}
