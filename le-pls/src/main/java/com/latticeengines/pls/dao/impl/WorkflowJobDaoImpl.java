package com.latticeengines.pls.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.WorkflowJob;
import com.latticeengines.pls.dao.WorkflowJobDao;

@Component("workflowJobDao")
public class WorkflowJobDaoImpl extends BaseDaoImpl<WorkflowJob> implements WorkflowJobDao {

    @Override
    protected Class<WorkflowJob> getEntityClass() {
        return WorkflowJob.class;
    }
}
