package com.latticeengines.workflow.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.workflow.exposed.service.WorkflowTenantService;

@Component("workflowTenantService")
public class WorkflowTenantServiceImpl implements WorkflowTenantService {

    private static final Log log = LogFactory.getLog(WorkflowTenantService.class);

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public Tenant getTenantFromConfiguration(WorkflowConfiguration workflowConfiguration) {
        if (workflowConfiguration == null || workflowConfiguration.getCustomerSpace() == null) {
            throw new LedpException(LedpCode.LEDP_28021);
        }
        return tenantEntityMgr.findByTenantId(workflowConfiguration.getCustomerSpace().toString());
    }

    @Override
    public Tenant getTenantByTenantPid(long tenantPid) {
        Tenant tenant = new Tenant();
        tenant.setPid(tenantPid);
        Tenant tenantWithPid = tenantEntityMgr.findByKey(tenant);
        if (tenantWithPid == null) {
            log.info("Could not find tenant with id:" + tenantPid);
            throw new LedpException(LedpCode.LEDP_28016, new String[] { String.valueOf(tenantPid) });
        }

        log.info("Looking for workflows for tenant: " + tenantWithPid.toString());
        return tenantWithPid;
    }
}
