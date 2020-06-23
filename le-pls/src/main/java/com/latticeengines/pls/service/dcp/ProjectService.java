package com.latticeengines.pls.service.dcp;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectRequest;
import com.latticeengines.domain.exposed.dcp.ProjectSummary;

public interface ProjectService {

    ProjectDetails createProject(String customerSpace, ProjectRequest projectRequest, String user);

    List<ProjectSummary> getAllProjects(String customerSpace, Boolean includeSources);

    ProjectDetails getProjectByProjectId(String customerSpace, String projectId, Boolean includeSources);

    void deleteProject(String customerSpace, String projectId);
}
