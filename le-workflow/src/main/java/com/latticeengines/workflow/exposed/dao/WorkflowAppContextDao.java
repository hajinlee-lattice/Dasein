package com.latticeengines.workflow.exposed.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowAppContext;

public interface WorkflowAppContextDao extends BaseDao<WorkflowAppContext> {

    List<WorkflowAppContext> findWorkflowIdsByTenant(Tenant tenant);

}
