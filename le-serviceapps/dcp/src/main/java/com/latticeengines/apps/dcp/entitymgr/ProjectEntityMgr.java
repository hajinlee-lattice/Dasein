package com.latticeengines.apps.dcp.entitymgr;

import java.util.List;

import org.springframework.data.domain.Pageable;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectInfo;

public interface ProjectEntityMgr extends BaseEntityMgrRepository<Project, Long> {

    Project findByProjectId(String projectId);

    ProjectInfo findProjectInfoByProjectId(String projectId);

    List<ProjectInfo> findAllProjectInfo(Pageable pageable, Boolean includeArchived);

    Long countAllProjects();

    Long countAllActiveProjects();

    ProjectInfo findProjectInfoBySourceId(String sourceId);

    List<ProjectInfo> findAllProjectInfoInTeamIds(Pageable pageable, List<String> teamIds);

    List<ProjectInfo> findAllProjectInfoInTeamIds(Pageable pageable, List<String> teamIds, Boolean includeArchived);

    ProjectInfo findProjectInfoByProjectIdInTeamIds(String projectId, List<String> teamIds);
}
