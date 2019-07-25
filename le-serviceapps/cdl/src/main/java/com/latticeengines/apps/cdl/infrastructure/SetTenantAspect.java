package com.latticeengines.apps.cdl.infrastructure;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;

@Aspect
public class SetTenantAspect {
    private static final Logger log = LoggerFactory.getLogger(SetTenantAspect.class);

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Before("execution(* com.latticeengines.apps.cdl.service.impl.CDLJobServiceImpl.create*(..)) ")
    public void allCreateDataFeedJobService(JoinPoint joinPoint) {
        String customerSpace = (String) joinPoint.getArgs()[0];
        setMultiTenantContext(CustomerSpace.parse(customerSpace).toString());
    }

    @Before("execution(* com.latticeengines.apps.cdl.service.impl.CDLExternalSystemServiceImpl.*(..))")
    public void allCDLExternalSystemService(JoinPoint joinPoint) {
        String customerSpace = (String) joinPoint.getArgs()[0];
        setMultiTenantContext(CustomerSpace.parse(customerSpace).toString());
    }

    @Before("execution(* com.latticeengines.apps.cdl.service.impl.S3ImportSystemServiceImpl.*(..))")
    public void allS3ImportSystemService(JoinPoint joinPoint) {
        String customerSpace = (String) joinPoint.getArgs()[0];
        setMultiTenantContext(CustomerSpace.parse(customerSpace).toString());
    }

    @Before("execution(* com.latticeengines.apps.cdl.service.impl.MigrateTrackingServiceImpl.*(..))")
    public void allMigrateTrackingService(JoinPoint joinPoint) {
        String customerSpace = (String) joinPoint.getArgs()[0];
        setMultiTenantContext(CustomerSpace.parse(customerSpace).toString());
    }

    @Before("execution(* com.latticeengines.apps.cdl.service.impl.AtlasExportServiceImpl.*(..))")
    public void allAtlasExportService(JoinPoint joinPoint) {
        String customerSpace = (String) joinPoint.getArgs()[0];
        setMultiTenantContext(CustomerSpace.parse(customerSpace).toString());
    }
    // ===================================
    // BEGIN: legacy aspects to be removed
    // ===================================
    @Before("execution(* com.latticeengines.apps.cdl.service.impl.StatisticsContainerServiceImpl.*(..))"
            + "&& !@annotation(com.latticeengines.apps.core.annotation.NoCustomerSpace)")
    public void allMethodsStatisticsContainerService(JoinPoint joinPoint) {
        String customerSpace = (String) joinPoint.getArgs()[0];
        setMultiTenantContext(customerSpace);
    }

    @Before("execution(* com.latticeengines.apps.cdl.service.impl.DataFeedServiceImpl.*(..)) "
            + "&& !execution(* com.latticeengines.apps.cdl.service.impl.DataFeedServiceImpl.getAllDataFeeds(..)) "
            + "&& !execution(* com.latticeengines.apps.cdl.service.impl.DataFeedServiceImpl.getAllSimpleDataFeeds(..)) "
            + "&& !execution(* com.latticeengines.apps.cdl.service.impl.DataFeedServiceImpl.getSimpleDataFeeds(..)) "
            + "&& !execution(* com.latticeengines.apps.cdl.service.impl.DataFeedServiceImpl.getDataFeeds(..)) "
            + "&& !execution(* com.latticeengines.apps.cdl.service.impl.DataFeedServiceImpl.getDataFeedsBySchedulingGroup(..)) "
            + "&& !execution(* com.latticeengines.apps.cdl.service.impl.DataFeedServiceImpl.getDataQuotaLimitMap(..)) "
            + "&& !@annotation(com.latticeengines.apps.core.annotation.NoCustomerSpace)")
    public void allMethodsDataFeedService(JoinPoint joinPoint) {
        String customerSpace = (String) joinPoint.getArgs()[0];
        setMultiTenantContext(customerSpace);
    }

    @Before("execution(* com.latticeengines.apps.cdl.service.impl.DataFeedTaskServiceImpl.*(..)) "
            + "&& !@annotation(com.latticeengines.apps.core.annotation.NoCustomerSpace)")
    public void allMethodsDataFeedTaskService(JoinPoint joinPoint) {
        String customerSpace = (String) joinPoint.getArgs()[0];
        setMultiTenantContext(customerSpace);
    }
    // ===================================
    // END: legacy aspects to be removed
    // ===================================

    private void setMultiTenantContext(String customerSpace) {
        Tenant tenant = tenantEntityMgr.findByTenantId(CustomerSpace.parse(customerSpace).toString());
        if (tenant == null) {
            throw new RuntimeException(String.format("No tenant found with id %s", customerSpace));
        }
        MultiTenantContext.setTenant(tenant);
    }
}
