package com.latticeengines.workflow.exposed.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowAppContext;

public interface WorkflowAppContextEntityMgr extends BaseEntityMgr<WorkflowAppContext> {

    List<WorkflowAppContext> findWorkflowIdsByTenant(Tenant tenant);
}
