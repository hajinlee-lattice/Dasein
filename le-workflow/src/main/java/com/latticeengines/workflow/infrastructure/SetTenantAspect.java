package com.latticeengines.workflow.infrastructure;

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

    @Before("@annotation(com.latticeengines.common.exposed.workflow.annotation.WithCustomerSpace)")
    public void allMethodsWorkflowJobService(JoinPoint joinPoint) {
        if (joinPoint.getArgs()[0] != null) {
            String customerSpace = String.valueOf(joinPoint.getArgs()[0]);
            setMultiTenantContext(CustomerSpace.parse(customerSpace).toString());
        } else {
            setMultiTenantContext(null);
        }
    }

    private void setMultiTenantContext(String customerSpace) {
        if (customerSpace != null) {
            Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace);
            if (tenant == null) {
                throw new RuntimeException(String.format("No tenant found with id %s", customerSpace));
            }
            MultiTenantContext.setTenant(tenant);
        } else {
            MultiTenantContext.setTenant(null);
        }
    }
}
