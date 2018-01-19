package com.latticeengines.workflow.entitymanager.impl;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.entitymanager.impl.MultiTenantEntityMgrAspect;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Aspect
public class WorkflowMultiTenantEntityMgrAspect extends MultiTenantEntityMgrAspect {

    @Autowired
    private SessionFactory sessionFactory;

    @Autowired
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
