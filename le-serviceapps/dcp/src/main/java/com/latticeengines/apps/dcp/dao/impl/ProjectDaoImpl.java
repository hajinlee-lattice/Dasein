package com.latticeengines.apps.dcp.dao.impl;

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
}
