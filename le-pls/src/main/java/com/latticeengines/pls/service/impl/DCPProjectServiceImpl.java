package com.latticeengines.pls.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dcp.DCPProject;
import com.latticeengines.domain.exposed.dcp.DCPProjectDetails;
import com.latticeengines.pls.service.DCPProjectService;
import com.latticeengines.proxy.exposed.cdl.DCPProjectProxy;


@Component("dcpService")
public class DCPProjectServiceImpl implements DCPProjectService {

    private static final Logger log = LoggerFactory.getLogger(DCPProjectServiceImpl.class);

    @Inject
    private DCPProjectProxy dcpProjectProxy;

    @Override
    public DCPProjectDetails createDCPProject(String customerSpace, String projectId, String displayName, DCPProject.ProjectType projectType, String user) {
        return dcpProjectProxy.createDCPProject(customerSpace, projectId, displayName, projectType, user);
    }

    @Override
    public List<DCPProject> getAllDCPProject(String customerSpace) {
        return dcpProjectProxy.getAllDCPProject(customerSpace);
    }

    @Override
    public DCPProjectDetails getDCPProjectByProjectId(String customerSpace, String projectId) {
        return dcpProjectProxy.getDCPProjectByProjectId(customerSpace, projectId);
    }

    @Override
    public void deleteProject(String customerSpace, String projectId) {
        dcpProjectProxy.deleteProject(customerSpace, projectId);
    }


}
