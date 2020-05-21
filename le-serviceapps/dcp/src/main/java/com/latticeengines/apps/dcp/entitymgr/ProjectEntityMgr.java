package com.latticeengines.apps.dcp.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectInfo;

public interface ProjectEntityMgr extends BaseEntityMgrRepository<Project, Long> {

    Project findByProjectId(String projectId);

    ProjectInfo findProjectInfoByProjectId(String projectId);

    List<ProjectInfo> findAllProjectInfo();

    ProjectInfo findProjectInfoBySourceId(String sourceId);

    S3ImportSystem findImportSystemByProjectId(String projectId);
}
