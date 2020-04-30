package com.latticeengines.pls.service.dcp;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectRequest;

public interface ProjectService {

    ProjectDetails createProject(String customerSpace, ProjectRequest projectRequest, String user);

    List<ProjectDetails> getAllProjects(String customerSpace);

    ProjectDetails getProjectByProjectId(String customerSpace, String projectId);

    void deleteProject(String customerSpace, String projectId);
}
