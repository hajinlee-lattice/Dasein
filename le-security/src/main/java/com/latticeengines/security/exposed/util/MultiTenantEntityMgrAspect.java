package com.latticeengines.security.exposed.util;

import java.util.Collections;
import java.util.List;

import javax.persistence.EntityManager;

import org.apache.commons.collections4.CollectionUtils;
import org.aspectj.lang.JoinPoint;
import org.hibernate.Filter;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.security.Tenant;

public class MultiTenantEntityMgrAspect {
    private static final Logger log = LoggerFactory.getLogger(MultiTenantEntityMgrAspect.class);
    
    public void enableMultiTenantFilter(JoinPoint joinPoint, SessionFactory sessionFactory,
            TenantEntityMgr tenantEntityMgr) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr, Collections.emptyList());
    }
    
    public void enableMultiTenantFilter(JoinPoint joinPoint, SessionFactory sessionFactory,
                                         TenantEntityMgr tenantEntityMgr, EntityManager entityManager) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr, Collections.singletonList(entityManager));
    }

    public void enableMultiTenantFilter(JoinPoint joinPoint, SessionFactory sessionFactory,
                                        TenantEntityMgr tenantEntityMgr, List<EntityManager> entityManagers) {

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

        if (CollectionUtils.isNotEmpty(entityManagers)) {
            entityManagers.forEach(entityManager -> {
                Session session = entityManager.unwrap(Session.class);
                if (session != null) {
                    Filter filter = session.enableFilter("tenantFilter");
                    filter.setParameter("tenantFilterId", tenant.getPid());
                    log.info("Turn on multi tenant filter: tenantId=" + tenant.getId());
                }
            });
        }

    }

}
