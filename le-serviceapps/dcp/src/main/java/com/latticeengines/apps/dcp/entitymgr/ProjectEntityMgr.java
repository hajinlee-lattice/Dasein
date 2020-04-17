package com.latticeengines.apps.dcp.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.dcp.Project;

public interface ProjectEntityMgr extends BaseEntityMgrRepository<Project, Long> {

    Project findByProjectId(String projectId);

    Project findByImportSystem(S3ImportSystem importSystem);
}
