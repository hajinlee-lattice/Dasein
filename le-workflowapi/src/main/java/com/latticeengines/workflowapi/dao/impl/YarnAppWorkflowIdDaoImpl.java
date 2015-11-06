package com.latticeengines.workflowapi.dao.impl;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.workflow.WorkflowId;
import com.latticeengines.domain.exposed.workflow.YarnAppWorkflowId;
import com.latticeengines.workflowapi.dao.YarnAppWorkflowIdDao;

@Component("yarnAppWorkflowIdDao")
public class YarnAppWorkflowIdDaoImpl extends BaseDaoImpl<YarnAppWorkflowId> implements YarnAppWorkflowIdDao {

    public YarnAppWorkflowIdDaoImpl() {
        super();
    }

    @Override
    protected Class<YarnAppWorkflowId> getEntityClass() {
        return YarnAppWorkflowId.class;
    }

    @Override
    public WorkflowId findWorkflowIdByApplicationId(ApplicationId appId) {
        Session session = sessionFactory.getCurrentSession();
        Query query = session.createQuery("from " + YarnAppWorkflowId.class.getSimpleName() + " Y where Y.yarnAppId=:anAppID");
        query.setString("anAppID", appId.toString());
        YarnAppWorkflowId yarnAppWorkflowId = (YarnAppWorkflowId) query.uniqueResult();
        return yarnAppWorkflowId.getAsWorkflowId();
    }

}