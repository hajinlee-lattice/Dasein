package com.latticeengines.pls.service.impl.dcp;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.pls.service.dcp.ProjectService;
import com.latticeengines.proxy.exposed.dcp.ProjectProxy;


@Component("projectService")
public class ProjectServiceImpl implements ProjectService {

    private static final Logger log = LoggerFactory.getLogger(ProjectServiceImpl.class);

    @Inject
    private ProjectProxy projectProxy;

    @Override
    public ProjectDetails createProject(String customerSpace, String projectId, String displayName, Project.ProjectType projectType, String user) {
        return projectProxy.createDCPProject(customerSpace, projectId, displayName, projectType, user);
    }

    @Override
    public List<Project> getAllProjects(String customerSpace) {
        return projectProxy.getAllDCPProject(customerSpace);
    }

    @Override
    public ProjectDetails getProjectByProjectId(String customerSpace, String projectId) {
        return projectProxy.getDCPProjectByProjectId(customerSpace, projectId);
    }

    @Override
    public void deleteProject(String customerSpace, String projectId) {
        projectProxy.deleteProject(customerSpace, projectId);
    }


}
