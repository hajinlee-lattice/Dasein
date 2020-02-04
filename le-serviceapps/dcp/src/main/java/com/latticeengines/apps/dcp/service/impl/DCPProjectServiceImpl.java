package com.latticeengines.apps.dcp.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.dcp.entitymgr.DCPProjectEntityMgr;
import com.latticeengines.apps.dcp.service.DCPProjectService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.DropBoxAccessMode;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessRequest;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.dcp.DCPProject;
import com.latticeengines.domain.exposed.dcp.DCPProjectDetails;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;

@Service("dcpProjectService")
public class DCPProjectServiceImpl implements DCPProjectService {

    private static final Logger log = LoggerFactory.getLogger(DCPProjectServiceImpl.class);

    private static final String PROJECT_ROOT_PATH_PATTERN = "/%s/Projects/%s/";
    private static final String RANDOM_PROJECT_ID_PATTERN = "Project_%s";
    private static final int MAX_RETRY = 3;

    @Inject
    private DCPProjectEntityMgr dcpProjectEntityMgr;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Override
    public DCPProjectDetails createDCPProject(String customerSpace, String displayName,
                                              DCPProject.ProjectType projectType, String user) {
        String projectId = generateRandomProjectId();
        String rootPath = generateRootPath(customerSpace, projectId);
        dcpProjectEntityMgr.create(generateDCPProjectObject(projectId, displayName, projectType, user, rootPath));
        DCPProject dcpProject = getDCPProjectByProjectIdWithRetry(projectId);
        if (dcpProject == null) {
            throw new RuntimeException(String.format("Create DCP Project %s failed!", displayName));
        }
        return getProjectDetails(customerSpace, dcpProject);
    }

    @Override
    public DCPProjectDetails createDCPProject(String customerSpace, String projectId, String displayName,
                                              DCPProject.ProjectType projectType, String user) {
        validateProjectId(projectId);
        String rootPath = generateRootPath(customerSpace, projectId);
        dcpProjectEntityMgr.create(generateDCPProjectObject(projectId, displayName, projectType, user, rootPath));
        DCPProject dcpProject = getDCPProjectByProjectIdWithRetry(projectId);
        if (dcpProject == null) {
            throw new RuntimeException(String.format("Create DCP Project %s failed!", displayName));
        }
        return getProjectDetails(customerSpace, dcpProject);
    }

    @Override
    public List<DCPProject> getAllDCPProject(String customerSpace) {
        return null;
    }

    @Override
    public DCPProjectDetails getDCPProjectByProjectId(String customerSpace, String projectId) {
        return null;
    }

    @Override
    public Boolean deleteProject(String customerSpace, String projectId) {
        return null;
    }

    private void validateProjectId(String projectId) {
        if (StringUtils.isBlank(projectId)) {
            throw new RuntimeException("Cannot create DCP project with blank projectId!");
        }
        if (!projectId.matches("[A-Za-z0-9_]*")) {
            throw new RuntimeException("Invalid characters in project id, only accept digits, alphabet & underline.");
        }
        if (dcpProjectEntityMgr.findByProjectId(projectId) != null) {
            throw new RuntimeException(String.format("ProjectId %s already exists.", projectId));
        }
    }

    private String generateRandomProjectId() {
        String randomProjectId = String.format(RANDOM_PROJECT_ID_PATTERN,
                RandomStringUtils.randomAlphanumeric(8).toLowerCase());
        while (dcpProjectEntityMgr.findByProjectId(randomProjectId) != null) {
            randomProjectId = String.format(RANDOM_PROJECT_ID_PATTERN,
                    RandomStringUtils.randomAlphanumeric(8).toLowerCase());
        }
        return randomProjectId;
    }

    private DCPProjectDetails getProjectDetails(String customerSpace, DCPProject dcpProject) {
        DCPProjectDetails details = new DCPProjectDetails();
        details.setProjectId(dcpProject.getProjectId());
        details.setProjectDisplayName(dcpProject.getProjectDisplayName());
        details.setProjectRootPath(dcpProject.getRootPath());
        details.setDropFolderAccess(getDropBoxAccess(customerSpace));
        return details;
    }

    private DCPProject generateDCPProjectObject(String projectId, String displayName,
                                                DCPProject.ProjectType projectType, String user, String rootPath) {
        DCPProject dcpProject = new DCPProject();
        dcpProject.setCreatedBy(user);
        dcpProject.setProjectDisplayName(displayName);
        dcpProject.setProjectId(projectId);
        dcpProject.setTenant(MultiTenantContext.getTenant());
        dcpProject.setDeleted(Boolean.FALSE);
        dcpProject.setProjectType(projectType);
        dcpProject.setRootPath(rootPath);
        return dcpProject;
    }

    private GrantDropBoxAccessResponse getDropBoxAccess(String customerSpace) {
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(customerSpace);
        if (StringUtils.isEmpty(dropBoxSummary.getAccessKeyId())) {
            GrantDropBoxAccessRequest request = new GrantDropBoxAccessRequest();
            request.setAccessMode(DropBoxAccessMode.LatticeUser);
            return dropBoxProxy.grantAccess(customerSpace, request);
        } else {
            return dropBoxProxy.getAccessKey(customerSpace);
        }
    }

    private String generateRootPath(String customerSpace, String projectId) {
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(customerSpace);
        return String.format(PROJECT_ROOT_PATH_PATTERN, dropBoxSummary.getDropBox(), projectId);
    }

    private DCPProject getDCPProjectByProjectIdWithRetry(String projectId) {
        int retry = 0;
        DCPProject dcpProject = dcpProjectEntityMgr.findByProjectId(projectId);
        while (dcpProject == null && retry < MAX_RETRY) {
            try {
                Thread.sleep(1000L + retry * 1000L);
            } catch (InterruptedException e) {
                return null;
            }
            dcpProject = dcpProjectEntityMgr.findByProjectId(projectId);
            retry++;
        }
        return dcpProject;
    }

}
