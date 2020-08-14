package com.latticeengines.apps.dcp.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectInfo;
import com.latticeengines.domain.exposed.dcp.ProjectSummary;

public interface ProjectService {

    ProjectDetails createProject(String customerSpace, String displayName,
                                 Project.ProjectType projectType, String user);

    ProjectDetails createProject(String customerSpace, String projectId, String displayName,
                                 Project.ProjectType projectType, String user);

    Project getProjectByProjectId(String customerSpace, String projectId);

    List<ProjectSummary> getAllProject(String customerSpace, Boolean includeSources, List<String> teamIds);

    List<ProjectSummary> getAllProject(String customerSpace, Boolean includeSources, Boolean includeArchived,
                                       int pageIndex, int pageSize,  List<String> teamIds);

    Long getProjectsCount(String customerSpace);

    ProjectDetails getProjectDetailByProjectId(String customerSpace, String projectId, Boolean includeSources, List<String> teamIds);

    Boolean deleteProject(String customerSpace, String projectId, List<String> teamIds);

    void updateRecipientList(String customerSpace, String projectId, List<String> recipientList);

    ProjectInfo getProjectBySourceId(String customerSpace, String sourceId);

    ProjectInfo getProjectInfoByProjectId(String customerSpace, String projectId);

    GrantDropBoxAccessResponse getDropFolderAccessByProjectId(String customerSpace, String projectId);

    void updateTeamId(String customerSpace, String projectId, String teamId);
}
