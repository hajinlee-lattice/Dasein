package com.latticeengines.apps.dcp.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.dcp.DCPProject;

public interface DCPProjectEntityMgr extends BaseEntityMgrRepository<DCPProject, Long> {

    DCPProject findByProjectId(String projectId);
}
