package com.latticeengines.pls.service.impl.dcp;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dcp.DCPProject;
import com.latticeengines.domain.exposed.dcp.DCPProjectDetails;
import com.latticeengines.pls.service.dcp.ProjectService;
import com.latticeengines.proxy.exposed.dcp.ProjectProxy;


@Component("dcpService")
public class ProjectServiceImpl implements ProjectService {

    private static final Logger log = LoggerFactory.getLogger(ProjectServiceImpl.class);

    @Inject
    private ProjectProxy projectProxy;

    @Override
    public DCPProjectDetails createDCPProject(String customerSpace, String projectId, String displayName, DCPProject.ProjectType projectType, String user) {
        return projectProxy.createDCPProject(customerSpace, projectId, displayName, projectType, user);
    }

    @Override
    public List<DCPProject> getAllDCPProject(String customerSpace) {
        return projectProxy.getAllDCPProject(customerSpace);
    }

    @Override
    public DCPProjectDetails getDCPProjectByProjectId(String customerSpace, String projectId) {
        return projectProxy.getDCPProjectByProjectId(customerSpace, projectId);
    }

    @Override
    public void deleteProject(String customerSpace, String projectId) {
        projectProxy.deleteProject(customerSpace, projectId);
    }


}
