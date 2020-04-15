package com.latticeengines.apps.dcp.dao.impl;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.dcp.dao.ProjectDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.dcp.Project;

@Component("projectDao")
public class ProjectDaoImpl extends BaseDaoImpl<Project> implements ProjectDao {

    @Override
    protected Class<Project> getEntityClass() {
        return Project.class;
    }

    @Override
    public void updateRecipientListByProjectId(String projectId, String recipientList) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Project> entityClz = getEntityClass();
        String queryStr = String.format(
                "update %s project set project.recipientList=:recipientList where project.projectId=:projectId",
                entityClz.getSimpleName());
        @SuppressWarnings("unchecked")
        Query<Project> query = session.createQuery(queryStr);
        query.setParameter("recipientList", recipientList);
        query.setParameter("projectId", projectId);
        query.executeUpdate();
    }
}
