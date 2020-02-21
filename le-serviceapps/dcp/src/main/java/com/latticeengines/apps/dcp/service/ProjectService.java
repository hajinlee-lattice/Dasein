package com.latticeengines.apps.dcp.service;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;

public interface ProjectService {

    ProjectDetails createProject(String customerSpace, String displayName,
                                 Project.ProjectType projectType, String user);

    ProjectDetails createProject(String customerSpace, String projectId, String displayName,
                                 Project.ProjectType projectType, String user);

    List<Project> getAllProject(String customerSpace);

    ProjectDetails getProjectByProjectId(String customerSpace, String projectId);

    Boolean deleteProject(String customerSpace, String projectId);

}
