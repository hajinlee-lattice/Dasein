package com.latticeengines.apps.dcp.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectSummary;

public interface ProjectService {

    ProjectDetails createProject(String customerSpace, String displayName,
                                 Project.ProjectType projectType, String user);

    ProjectDetails createProject(String customerSpace, String projectId, String displayName,
                                 Project.ProjectType projectType, String user);

    List<ProjectSummary> getAllProject(String customerSpace);

    Project getProjectByProjectId(String customerSpace, String projectId);

    Project getProjectByImportSystem(String customerSpace, S3ImportSystem importSystem);

    ProjectDetails getProjectDetailByProjectId(String customerSpace, String projectId);

    Boolean deleteProject(String customerSpace, String projectId);

    void updateRecipientList(String customerSpace, String projectId, List<String> recipientList);
}
