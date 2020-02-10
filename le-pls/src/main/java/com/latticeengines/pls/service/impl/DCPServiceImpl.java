package com.latticeengines.pls.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dcp.DCPProject;
import com.latticeengines.domain.exposed.dcp.DCPProjectDetails;
import com.latticeengines.pls.service.DCPService;
import com.latticeengines.proxy.exposed.cdl.DCPProxy;


@Component("dcpService")
public class DCPServiceImpl implements DCPService {

    private static final Logger log = LoggerFactory.getLogger(DCPServiceImpl.class);

    @Inject
    private DCPProxy dcpProxy;

    @Override
    public DCPProjectDetails createDCPProject(String customerSpace, String projectId, String displayName, DCPProject.ProjectType projectType, String user) {
        return dcpProxy.createDCPProject(customerSpace, projectId, displayName, projectType, user);
    }

    @Override
    public List<DCPProject> getAllDCPProject(String customerSpace) {
        return dcpProxy.getAllDCPProject(customerSpace);
    }

    @Override
    public DCPProjectDetails getDCPProjectByProjectId(String customerSpace, String projectId) {
        return dcpProxy.getDCPProjectByProjectId(customerSpace, projectId);
    }

    @Override
    public void deleteProject(String customerSpace, String projectId) {
        dcpProxy.deleteProject(customerSpace, projectId);
    }


}
