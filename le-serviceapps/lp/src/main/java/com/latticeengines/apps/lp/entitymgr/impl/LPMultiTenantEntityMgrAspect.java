package com.latticeengines.apps.lp.entitymgr.impl;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.apps.lp.service.ModelSummaryCacheService;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.util.MultiTenantEntityMgrAspect;

@Aspect
public class LPMultiTenantEntityMgrAspect extends MultiTenantEntityMgrAspect {

    private static final Logger log = LoggerFactory.getLogger(LPMultiTenantEntityMgrAspect.class);

    @Resource(name = "sessionFactory")
    private SessionFactory sessionFactory;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private ModelSummaryCacheService modelSummaryCacheService;

    private void clearModelSummaryCache(JoinPoint joinPoint) {
        Tenant tenant = MultiTenantContext.getTenant();
        String modelId = null;
        if (joinPoint.getArgs().length > 0) {
            if (joinPoint.getArgs()[0] instanceof ModelSummary) {
                ModelSummary modelSummary = (ModelSummary) joinPoint.getArgs()[0];
                if (modelSummary.getId() != null) {
                    modelId = modelSummary.getId();
                }

            } else if (joinPoint.getArgs()[0] instanceof String) {
                modelId = (String) joinPoint.getArgs()[0];
            }
        }
        if (tenant != null) {
            modelSummaryCacheService.clearCache(tenant, modelId);
        }
    }

    // redis delay clear model summary cache
    @After("@annotation(com.latticeengines.apps.core.annotation.ClearCache)")
    public void afterCUDModelSummay(JoinPoint joinPoint) {
        clearModelSummaryCache(joinPoint);
    }

    @Before("execution(* com.latticeengines.apps.lp.entitymgr.impl.ModelSummaryEntityMgrImpl.update*(..))")
    public void updateModelSummary(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.apps.lp.entitymgr.impl.ModelSummaryEntityMgrImpl.getAll(..))")
    public void getModelSummaries(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }
}
