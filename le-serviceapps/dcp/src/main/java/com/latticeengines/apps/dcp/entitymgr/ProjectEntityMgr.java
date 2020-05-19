package com.latticeengines.apps.dcp.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectInfo;

public interface ProjectEntityMgr extends BaseEntityMgrRepository<Project, Long> {

    Project findByProjectId(String projectId);

//    Project findByImportSystem(S3ImportSystem importSystem);

    ProjectInfo getProjectInfoByProjectId(String projectId);

    List<ProjectInfo> getAllProjectInfo();

    ProjectInfo getProjectInfoBySourceId(String sourceId);

    S3ImportSystem getImportSystemByProjectId(String projectId);
}
