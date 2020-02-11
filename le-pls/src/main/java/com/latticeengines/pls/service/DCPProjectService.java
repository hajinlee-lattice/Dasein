package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.DCPProject;
import com.latticeengines.domain.exposed.dcp.DCPProjectDetails;

public interface DCPProjectService {

    DCPProjectDetails createDCPProject(String customerSpace, String projectId, String displayName, DCPProject.ProjectType projectType, String user);

    List<DCPProject> getAllDCPProject(String customerSpace);

    DCPProjectDetails getDCPProjectByProjectId(String customerSpace, String projectId);

    void deleteProject(String customerSpace, String projectId);
}
