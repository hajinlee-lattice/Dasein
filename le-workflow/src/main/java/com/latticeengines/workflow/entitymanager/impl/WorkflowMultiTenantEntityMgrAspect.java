package com.latticeengines.workflow.entitymanager.impl;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.entitymanager.impl.MultiTenantEntityMgrAspect;

@Aspect
public class WorkflowMultiTenantEntityMgrAspect extends MultiTenantEntityMgrAspect {

    @Autowired
    private SessionFactory sessionFactory;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Before("execution(* com.latticeengines.workflow.entitymanager.impl.WorkflowJobEntityMgrImpl.findByWorkflowIdWithFilter(..))")
    public void findByWorkflowJobId(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.workflow.entitymanager.impl.WorkflowJobEntityMgrImpl.findByWorkflowIdsWithFilter(..))")
    public void findByWorkflowJobIds(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }
}
