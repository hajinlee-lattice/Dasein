package com.latticeengines.app.exposed.aspect;

import javax.inject.Inject;
import javax.persistence.EntityManagerFactory;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.orm.jpa.EntityManagerFactoryUtils;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.util.MultiTenantEntityMgrAspect;

@Aspect
public class AppMultiTenantEntityMgrAspect extends MultiTenantEntityMgrAspect {

    @Inject
    private SessionFactory sessionFactory;

    @Inject
    @Qualifier("datadb")
    private SessionFactory dataSessionFactory;

    @Inject
    @Qualifier("datadb")
    private EntityManagerFactory dataEntityManagerFactory;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Before("execution(* com.latticeengines.app.exposed.entitymanager.impl.*.*(..)) && "
            + "!execution(*  com.latticeengines.app.exposed.entitymanager.impl.FileDownloadEntityMgrImpl.getByToken(..))")
    // + " && !execution(*
    // com.latticeengines.app.exposed.entitymanager.impl.ActivityAlertEntityMgrImpl.*(..))")
    public void allEntityMgrMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.app.exposed.entitymanager.impl.ActivityAlertEntityMgrImpl.*(..)))")
    public void dataEntityMgrMethods(JoinPoint joinPoint) {
        Tenant tenant = MultiTenantContext.getTenant();

        if (tenant == null) {
            throw new RuntimeException("Problem with multi-tenancy framework");
        }

        EntityManagerFactoryUtils.getTransactionalEntityManager(dataEntityManagerFactory) //
                .unwrap(Session.class) //
                .enableFilter("tenantFilter").setParameter("tenantFilterId", tenant.getPid());
    }
}
