package com.latticeengines.workflow.exposed.service;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public interface WorkflowTenantService {

    Tenant getTenantFromConfiguration(WorkflowConfiguration workflowConfiguration);

    Tenant getTenantByTenantPid(long tenantPid);
}
