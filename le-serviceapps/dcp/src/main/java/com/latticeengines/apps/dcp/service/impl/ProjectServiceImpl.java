package com.latticeengines.apps.dcp.service.impl;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.core.service.DropBoxService;
import com.latticeengines.apps.dcp.entitymgr.ProjectEntityMgr;
import com.latticeengines.apps.dcp.service.ProjectService;
import com.latticeengines.apps.dcp.service.SourceService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.DropBoxAccessMode;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessRequest;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;

@Service("projectService")
public class ProjectServiceImpl implements ProjectService {

    private static final Logger log = LoggerFactory.getLogger(ProjectServiceImpl.class);

    private static final String PROJECT_ROOT_PATH_PATTERN = "Projects/%s/";
    private static final String RANDOM_PROJECT_ID_PATTERN = "Project_%s";
    private static final String SYSTEM_NAME_PATTERN = "ProjectSystem_%s";
    private static final int MAX_RETRY = 3;

    @Inject
    private ProjectEntityMgr projectEntityMgr;

    @Inject
    private CDLProxy cdlProxy;

    @Inject
    private DropBoxService dropBoxService;

    @Inject
    private SourceService sourceService;

    @Override
    public ProjectDetails createProject(String customerSpace, String displayName,
                                        Project.ProjectType projectType, String user) {
        String projectId = generateRandomProjectId();
        String rootPath = generateRootPath(projectId);
        S3ImportSystem system = createProjectSystem(customerSpace, displayName, projectId);
        projectEntityMgr.create(generateProjectObject(projectId, displayName, projectType, user, rootPath, system));
        Project project = getProjectByProjectIdWithRetry(projectId);
        if (project == null) {
            throw new RuntimeException(String.format("Create DCP Project %s failed!", displayName));
        }
        dropBoxService.createFolderUnderDropFolder(project.getRootPath());
        return getProjectDetails(customerSpace, project);
    }

    @Override
    public ProjectDetails createProject(String customerSpace, String projectId, String displayName,
                                        Project.ProjectType projectType, String user) {
        validateProjectId(projectId);
        String rootPath = generateRootPath(projectId);
        S3ImportSystem system = createProjectSystem(customerSpace, displayName, projectId);
        projectEntityMgr.create(generateProjectObject(projectId, displayName, projectType, user, rootPath, system));
        Project project = getProjectByProjectIdWithRetry(projectId);
        if (project == null) {
            throw new RuntimeException(String.format("Create DCP Project %s failed!", displayName));
        }
        dropBoxService.createFolderUnderDropFolder(project.getRootPath());
        return getProjectDetails(customerSpace, project);
    }

    @Override
    public List<Project> getAllProject(String customerSpace) {
        return projectEntityMgr.findAll();
    }

    @Override
    public Project getProjectByProjectId(String customerSpace, String projectId) {
        return getProjectByProjectIdWithRetry(projectId);
    }

    @Override
    public Project getProjectByImportSystem(String customerSpace, S3ImportSystem importSystem) {
        return projectEntityMgr.findByImportSystem(importSystem);
    }

    @Override
    public ProjectDetails getProjectDetailByProjectId(String customerSpace, String projectId) {
        Project project = getProjectByProjectIdWithRetry(projectId);
        if (project == null) {
            throw new RuntimeException(String.format("Get DCP Project %s failed!", projectId));
        }
        return getProjectDetails(customerSpace, project);
    }

    @Override
    public Boolean deleteProject(String customerSpace, String projectId) {
        Project project = getProjectByProjectIdWithRetry(projectId);
        if (project == null) {
            return false;
        }
        projectEntityMgr.delete(project);
        return true;
    }

    private void validateProjectId(String projectId) {
        if (StringUtils.isBlank(projectId)) {
            throw new RuntimeException("Cannot create DCP project with blank projectId!");
        }
        if (!projectId.matches("[A-Za-z0-9_]*")) {
            throw new RuntimeException("Invalid characters in project id, only accept digits, alphabet & underline.");
        }
        if (projectEntityMgr.findByProjectId(projectId) != null) {
            throw new RuntimeException(String.format("ProjectId %s already exists.", projectId));
        }
    }

    private String generateRandomProjectId() {
        String randomProjectId = String.format(RANDOM_PROJECT_ID_PATTERN,
                RandomStringUtils.randomAlphanumeric(8).toLowerCase());
        while (projectEntityMgr.findByProjectId(randomProjectId) != null) {
            randomProjectId = String.format(RANDOM_PROJECT_ID_PATTERN,
                    RandomStringUtils.randomAlphanumeric(8).toLowerCase());
        }
        return randomProjectId;
    }

    private ProjectDetails getProjectDetails(String customerSpace, Project project) {
        ProjectDetails details = new ProjectDetails();
        details.setProjectId(project.getProjectId());
        details.setProjectDisplayName(project.getProjectDisplayName());
        details.setProjectRootPath(project.getRootPath());
        details.setDropFolderAccess(getDropBoxAccess());
        if (project.getS3ImportSystem() != null && CollectionUtils.isNotEmpty(project.getS3ImportSystem().getTasks())) {
            details.setSources(new ArrayList<>());
            project.getS3ImportSystem().getTasks()
                    .forEach(task -> {
                        if (!Boolean.TRUE.equals(task.getDeleted())) {
                            details.getSources().add(sourceService.convertToSource(customerSpace, task));
                        }
                    });
        }
        return details;
    }

    private Project generateProjectObject(String projectId, String displayName,
                                          Project.ProjectType projectType, String user, String rootPath,
                                          S3ImportSystem system) {
        Project project = new Project();
        project.setCreatedBy(user);
        project.setProjectDisplayName(displayName);
        project.setProjectId(projectId);
        project.setTenant(MultiTenantContext.getTenant());
        project.setDeleted(Boolean.FALSE);
        project.setProjectType(projectType);
        project.setRootPath(rootPath);
        project.setS3ImportSystem(system);
        return project;
    }

    private S3ImportSystem createProjectSystem(String customerSpace, String displayName, String projectId) {
        S3ImportSystem system = new S3ImportSystem();
        system.setTenant(MultiTenantContext.getTenant());
        String systemName = String.format(SYSTEM_NAME_PATTERN, projectId);
        system.setName(systemName);
        system.setDisplayName(displayName);
        system.setSystemType(S3ImportSystem.SystemType.ProjectSystem);
        cdlProxy.createS3ImportSystem(customerSpace, system);
        system = cdlProxy.getS3ImportSystem(customerSpace, systemName);
        int retry = 0;
        while (system == null && retry < MAX_RETRY) {
            try {
                Thread.sleep(1000L + retry * 1000L);
            } catch (InterruptedException e) {
                return null;
            }
            system = cdlProxy.getS3ImportSystem(customerSpace, systemName);
            retry++;
        }
        if (system == null) {
            throw new RuntimeException("Cannot create DCP Project due to ImportSystem creation error!");
        }
        return system;
    }

    private GrantDropBoxAccessResponse getDropBoxAccess() {
        DropBoxSummary dropBoxSummary = dropBoxService.getDropBoxSummary();
        if (StringUtils.isEmpty(dropBoxSummary.getAccessKeyId())) {
            GrantDropBoxAccessRequest request = new GrantDropBoxAccessRequest();
            request.setAccessMode(DropBoxAccessMode.LatticeUser);
            return dropBoxService.grantAccess(request);
        } else {
            return dropBoxService.getAccessKey();
        }
    }

    private String generateRootPath(String projectId) {
        return String.format(PROJECT_ROOT_PATH_PATTERN, projectId);
    }

    private Project getProjectByProjectIdWithRetry(String projectId) {
        int retry = 0;
        Project project = projectEntityMgr.findByProjectId(projectId);
        while (project == null && retry < MAX_RETRY) {
            try {
                Thread.sleep(1000L + retry * 1000L);
            } catch (InterruptedException e) {
                return null;
            }
            project = projectEntityMgr.findByProjectId(projectId);
            retry++;
        }
        return project;
    }

}
