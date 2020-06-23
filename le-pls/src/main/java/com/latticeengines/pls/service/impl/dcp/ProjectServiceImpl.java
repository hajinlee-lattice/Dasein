package com.latticeengines.pls.service.impl.dcp;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectRequest;
import com.latticeengines.domain.exposed.dcp.ProjectSummary;
import com.latticeengines.pls.service.dcp.ProjectService;
import com.latticeengines.proxy.exposed.dcp.ProjectProxy;


@Component("projectService")
public class ProjectServiceImpl implements ProjectService {

    private static final Logger log = LoggerFactory.getLogger(ProjectServiceImpl.class);

    @Inject
    private ProjectProxy projectProxy;

    @Override
    public ProjectDetails createProject(String customerSpace, ProjectRequest projectRequest, String user) {
        return projectProxy.createDCPProject(customerSpace, projectRequest, user);
    }

    @Override
    public List<ProjectSummary> getAllProjects(String customerSpace) {
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

    @Override
    public GrantDropBoxAccessResponse getDropFolderAccessByProjectId(String customerSpace, String projectId) {
        return projectProxy.getDropFolderAccessByProjectId(customerSpace, projectId);
    }

}
