package com.latticeengines.apps.dcp.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectInfo;
import com.latticeengines.domain.exposed.dcp.ProjectSummary;

public interface ProjectService {

    ProjectDetails createProject(String customerSpace, String displayName,
                                 Project.ProjectType projectType, String user);

    ProjectDetails createProject(String customerSpace, String projectId, String displayName,
                                 Project.ProjectType projectType, String user);

    List<ProjectSummary> getAllProject(String customerSpace);

    ProjectDetails getProjectDetailByProjectId(String customerSpace, String projectId);

    Boolean deleteProject(String customerSpace, String projectId);

    void updateRecipientList(String customerSpace, String projectId, List<String> recipientList);

    ProjectInfo getProjectBySourceId(String customerSpace, String sourceId);

    ProjectInfo getProjectInfoByProjectId(String customerSpace, String projectId);

    S3ImportSystem getImportSystemByProjectId(String customerSpace, String projectId);
}
