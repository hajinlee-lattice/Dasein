package com.latticeengines.apps.dcp.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.dcp.dao.ProjectSystemLinkDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.dcp.ProjectSystemLink;

@Component("projectSystemLinkDao")
public class ProjectSystemLinkDaoImpl extends BaseDaoImpl<ProjectSystemLink> implements ProjectSystemLinkDao {

    @Override
    protected Class<ProjectSystemLink> getEntityClass() {
        return ProjectSystemLink.class;
    }
}
