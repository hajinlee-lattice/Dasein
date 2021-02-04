package com.latticeengines.metadata.infrastructure;

import javax.inject.Inject;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.entitymgr.impl.TableTypeHolder;

@Aspect
public class ResourceAspect {

    @Inject
    private TableTypeHolder tableTypeHolder;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Around("execution(* com.latticeengines.metadata.controller.ImportTableResource.*(..))")
    public Object allMethodsForImportTableResource(ProceedingJoinPoint joinPoint) throws Throwable {
        setTableType(joinPoint, TableType.IMPORTTABLE);
        Object obj = joinPoint.proceed();
        setTableType(joinPoint, TableType.DATATABLE);
        return obj;
    }

    @Before("execution(* com.latticeengines.metadata.controller.TableResource.*(..))")
    public void allMethodsForTableResource(JoinPoint joinPoint) {
        setTableType(joinPoint, TableType.DATATABLE);
    }

    @Before("execution(* com.latticeengines.metadata.controller.DataUnitResource.*(..))")
    public void beforeAllMethodsForDataUnitResource(JoinPoint joinPoint) {
        String customerSpace = (String) joinPoint.getArgs()[0];
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        setMultiTenantContext(customerSpace);
    }

    @Before("execution(* com.latticeengines.metadata.controller.DataTemplateResource.*(..))")
    public void beforeAllMethodsForDataTemplateResource(JoinPoint joinPoint) {
        String customerSpace = (String) joinPoint.getArgs()[0];
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        setMultiTenantContext(customerSpace);
    }


    private void setTableType(JoinPoint joinPoint, TableType tableType) {
        tableTypeHolder.setTableType(tableType);
        Object[] args = joinPoint.getArgs();

        for (Object arg : args) {
            if (arg instanceof Table) {
                ((Table) arg).setTableType(tableType);
            }
        }
    }

    private void setMultiTenantContext(String customerSpace) {
        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace);
        if (tenant == null) {
            throw new RuntimeException(String.format("No tenant found with id %s", customerSpace));
        }
        MultiTenantContext.setTenant(tenant);
    }

}
