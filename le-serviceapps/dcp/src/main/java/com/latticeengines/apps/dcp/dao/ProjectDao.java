package com.latticeengines.apps.dcp.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.dcp.Project;

public interface ProjectDao extends BaseDao<Project> {
    void updateRecipientListByProjectId(String projectId, String recipientList);
}
