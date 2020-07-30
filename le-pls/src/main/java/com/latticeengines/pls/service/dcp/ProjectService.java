package com.latticeengines.pls.service.dcp;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectRequest;
import com.latticeengines.domain.exposed.dcp.ProjectSummary;

public interface ProjectService {

    ProjectDetails createProject(String customerSpace, ProjectRequest projectRequest, String user);

    List<ProjectSummary> getAllProjects(String customerSpace, Boolean includeSources);

    List<ProjectSummary> getAllProjects(String customerSpace, Boolean includeSources, int pageIndex, int pageSize);

    ProjectDetails getProjectByProjectId(String customerSpace, String projectId, Boolean includeSources);

    void deleteProject(String customerSpace, String projectId);

    GrantDropBoxAccessResponse getDropFolderAccessByProjectId(String toString, String projectId);
}
