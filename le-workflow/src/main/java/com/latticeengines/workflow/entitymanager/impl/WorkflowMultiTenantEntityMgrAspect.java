package com.latticeengines.workflow.entitymanager.impl;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.hibernate.SessionFactory;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.util.MultiTenantEntityMgrAspect;

@Aspect
public class WorkflowMultiTenantEntityMgrAspect extends MultiTenantEntityMgrAspect {

    @Resource(name = "sessionFactory")
    private SessionFactory sessionFactory;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public void enableMultiTenantFilter(JoinPoint joinPoint, SessionFactory sessionFactory,
                                        TenantEntityMgr tenantEntityMgr) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (tenant == null) {
            return;
        }
        super.enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.workflow.entitymanager.impl.WorkflowJobEntityMgrImpl.find*(..)))")
    public void workflowJobEntityMgrImplWithFilter(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }
}
