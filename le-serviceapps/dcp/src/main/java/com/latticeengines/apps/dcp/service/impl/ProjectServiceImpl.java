package com.latticeengines.apps.dcp.service.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.core.service.DropBoxService;
import com.latticeengines.apps.dcp.entitymgr.ProjectEntityMgr;
import com.latticeengines.apps.dcp.service.DataReportService;
import com.latticeengines.apps.dcp.service.ProjectService;
import com.latticeengines.apps.dcp.service.SourceService;
import com.latticeengines.apps.dcp.workflow.DCPDataReportWorkflowSubmitter;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.DropBoxAccessMode;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessRequest;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.dcp.DCPReportRequest;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportMode;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectInfo;
import com.latticeengines.domain.exposed.dcp.ProjectSummary;
import com.latticeengines.domain.exposed.dcp.ProjectUpdateRequest;
import com.latticeengines.domain.exposed.dcp.PurposeOfUse;

@Service("projectService")
public class ProjectServiceImpl extends ServiceCommonImpl implements ProjectService {

    private static final Logger log = LoggerFactory.getLogger(ProjectServiceImpl.class);

    private static final String PROJECT_ROOT_PATH_PATTERN = "Projects/%s/";
    private static final String RANDOM_PROJECT_ID_PATTERN = "Project_%s";
    private static final String FULL_PATH_PATTERN = "%s/%s/%s"; // {bucket}/{dropfolder}/{project path}
    private static final int MAX_RETRY = 3;

    @Inject
    private ProjectEntityMgr projectEntityMgr;

    @Inject
    private DropBoxService dropBoxService;

    @Inject
    private SourceService sourceService;

    @Inject
    private DataReportService dataReportService;

    @Inject
    private DCPDataReportWorkflowSubmitter dataReportWorkflowSubmitter;

    @Override
    public ProjectDetails createProject(String customerSpace, String displayName,
                                        Project.ProjectType projectType, String user, PurposeOfUse purposeOfUse, String description) {
        String projectId = generateRandomProjectId();
        String rootPath = generateRootPath(projectId);
        projectEntityMgr.create(generateProjectObject(projectId, displayName, projectType, user, rootPath,
                purposeOfUse, description));
        ProjectInfo project = getProjectInfoByProjectIdWithRetry(projectId);
        if (project == null) {
            throw new RuntimeException(String.format("Create DCP Project %s failed!", displayName));
        }
        dropBoxService.createFolderUnderDropFolder(project.getRootPath());
        return getProjectDetails(customerSpace, project, Boolean.FALSE);
    }

    @Override
    public ProjectDetails createProject(String customerSpace, String projectId, String displayName,
                                        Project.ProjectType projectType, String user, PurposeOfUse purposeOfUse, String description) {
        validateProjectId(projectId);
        String rootPath = generateRootPath(projectId);
        projectEntityMgr.create(generateProjectObject(projectId, displayName, projectType, user, rootPath,
                purposeOfUse, description));
        ProjectInfo project = getProjectInfoByProjectIdWithRetry(projectId);
        if (project == null) {
            throw new RuntimeException(String.format("Create DCP Project %s failed!", displayName));
        }
        dropBoxService.createFolderUnderDropFolder(project.getRootPath());
        return getProjectDetails(customerSpace, project, Boolean.FALSE);
    }

    @Override
    public Project getProjectByProjectId(String customerSpace, String projectId) {
        return projectEntityMgr.findByProjectId(projectId);
    }

    @Override
    public List<ProjectSummary> getAllProject(String customerSpace, Boolean includeSources, Boolean includeArchived,
                                              int pageIndex, int pageSize, List<String> teamIds) {
        log.info("Start getAllProject!");
        try (PerformanceTimer timer = new PerformanceTimer()) {
            PageRequest pageRequest = getPageRequest(pageIndex, pageSize);
            List<ProjectInfo> projectInfoList;
            if (teamIds == null){
                projectInfoList = projectEntityMgr.findAllProjectInfo(pageRequest, includeArchived);
            } else {
                if (teamIds.isEmpty()) {
                    teamIds.add("");
                }
                projectInfoList = projectEntityMgr.findAllProjectInfoInTeamIds(pageRequest, teamIds, includeArchived);
            }
            timer.setTimerMessage("Find " + CollectionUtils.size(projectInfoList) + " Projects in page.");
            Map<String, DataReport.BasicStats> basicStatsMap = dataReportService.getDataReportBasicStats(customerSpace,
                    DataReportRecord.Level.Project);
            return projectInfoList.stream().map(projectInfo -> getProjectSummary(customerSpace, projectInfo,
                    basicStatsMap.get(projectInfo.getProjectId()), includeSources)).collect(Collectors.toList());
        }
    }

    @Override
    public Long getProjectsCount(String customerSpace) {
        return projectEntityMgr.countAllProjects();
    }

    @Override
    public ProjectDetails getProjectDetailByProjectId(String customerSpace, String projectId, Boolean includeSources, List<String> teamIds) {
        RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(MAX_RETRY,
                Collections.singleton(IllegalArgumentException.class), null);
        ProjectInfo projectInfo = retryTemplate.execute(context -> {
            ProjectInfo info;
            if (teamIds == null){
                info = projectEntityMgr.findProjectInfoByProjectId(projectId);
            } else {
                if (teamIds.isEmpty()) {
                    teamIds.add("");
                }
                info = projectEntityMgr.findProjectInfoByProjectIdInTeamIds(projectId, teamIds);
            }
            if (info == null) {
                throw new IllegalArgumentException("Cannot find project with id: " + projectId);
            }
            return info;
        });
        if (projectInfo == null) {
            log.warn("No project found with id: " + projectId);
            return null;
        }
        return getProjectDetails(customerSpace, projectInfo, includeSources);
    }

    @Override
    public Boolean deleteProject(String customerSpace, String projectId, List<String> teamIds) {
        Project project = projectEntityMgr.findByProjectId(projectId);
        if (project == null || (project.getTeamId() != null && teamIds != null && !teamIds.contains(project.getTeamId()))) {
            return false;
        }

        log.info("soft delete the data report under {}", projectId);
        dataReportService.deleteDataReportUnderOwnerId(customerSpace, DataReportRecord.Level.Project,
                projectId);
        project.setDeleted(Boolean.TRUE);
        projectEntityMgr.update(project);
        return true;
    }

    @Override
    public Boolean hardDeleteProject(String customerSpace, String projectId, List<String> teamIds) {
        Project project = projectEntityMgr.findByProjectId(projectId);
        if (project == null || (project.getTeamId() != null && teamIds != null && !teamIds.contains(project.getTeamId()))) {
            return false;
        }

        sourceService.hardDeleteSourceUnderProject(customerSpace, projectId);

        log.info("hard delete the data report under {}", projectId);
        dataReportService.hardDeleteDataReportUnderOwnerId(customerSpace, DataReportRecord.Level.Project,
                projectId);
        projectEntityMgr.delete(project);

        dropBoxService.removeFolder(project.getRootPath());

        log.info("trigger roll up for tenant {} after true delete project", customerSpace);
        DCPReportRequest request = new DCPReportRequest();
        request.setLevel(DataReportRecord.Level.Tenant);
        request.setMode(DataReportMode.UPDATE);
        request.setRootId(CustomerSpace.parse(customerSpace).toString());

        dataReportWorkflowSubmitter.submit(CustomerSpace.parse(customerSpace), request, new WorkflowPidWrapper(-1L));

        return true;
    }

    @Override
    public void updateRecipientList(String customerSpace, String projectId, List<String> recipientList) {
        Project project = projectEntityMgr.findByProjectId(projectId);
        if (project == null) {
            return;
        }
        project.setRecipientList(recipientList);
        projectEntityMgr.update(project);
    }

    @Override
    public ProjectInfo getProjectBySourceId(String customerSpace, String sourceId) {
        return projectEntityMgr.findProjectInfoBySourceId(sourceId);
    }

    @Override
    public ProjectInfo getProjectInfoByProjectId(String customerSpace, String projectId) {
        return projectEntityMgr.findProjectInfoByProjectId(projectId);
    }

    @Override
    public GrantDropBoxAccessResponse getDropFolderAccessByProjectId(String customerSpace, String projectId) {
        return getDropBoxAccess();
    }

    @Override
    public void updateTeamId(String customerSpace, String projectId, String teamId) {
        Project project = projectEntityMgr.findByProjectId(projectId);
        if (project == null) {
            return;
        }
        project.setTeamId(teamId);
        projectEntityMgr.update(project);
    }

    @Override
    public void updateProject(String customerSpace, String projectId, ProjectUpdateRequest request) {
        Project project = projectEntityMgr.findByProjectId(projectId);
        if (null == project) {
            return;
        }
        boolean needUpdate = false;
        if (StringUtils.isNotBlank(request.getDisplayName())) {
            project.setProjectDisplayName(request.getDisplayName());
            needUpdate = true;
        }
        if (StringUtils.isNotBlank(request.getProjectDescription())) {
            project.setProjectDescription(request.getProjectDescription());
            needUpdate = true;
        }
        if (needUpdate) {
            projectEntityMgr.update(project);
        } else {
            log.info("no non-empty value in the request body for {} in {}", projectId, customerSpace);
        }
    }

    private void validateProjectId(String projectId) {
        if (StringUtils.isBlank(projectId)) {
            throw new RuntimeException("Cannot create DCP project with blank projectId!");
        }
        if (!projectId.matches("[A-Za-z0-9_]*")) {
            throw new RuntimeException("Invalid characters in project id, only accept digits, alphabet & underline.");
        }
        if (projectEntityMgr.findProjectInfoByProjectId(projectId) != null) {
            throw new RuntimeException(String.format("ProjectId %s already exists.", projectId));
        }
    }

    private String generateRandomProjectId() {
        String randomProjectId = String.format(RANDOM_PROJECT_ID_PATTERN,
                RandomStringUtils.randomAlphanumeric(8).toLowerCase());
        while (projectEntityMgr.findProjectInfoByProjectId(randomProjectId) != null) {
            randomProjectId = String.format(RANDOM_PROJECT_ID_PATTERN,
                    RandomStringUtils.randomAlphanumeric(8).toLowerCase());
        }
        return randomProjectId;
    }

    private ProjectDetails getProjectDetails(String customerSpace, ProjectInfo projectInfo, Boolean includeSources) {
        ProjectDetails details = new ProjectDetails();
        details.setProjectId(projectInfo.getProjectId());
        details.setProjectDisplayName(projectInfo.getProjectDisplayName());
        details.setProjectRootPath(projectInfo.getRootPath());
        details.setProjectFullPath(String.format(FULL_PATH_PATTERN, dropBoxService.getDropBoxBucket(),
                dropBoxService.getDropBoxPrefix(), projectInfo.getRootPath()));
        details.setDeleted(projectInfo.getDeleted());
        if (includeSources) {
            details.setSources(sourceService.getSourceList(customerSpace, projectInfo.getProjectId()));
            details.setTotalSourceCount(sourceService.getSourceCount(customerSpace, projectInfo.getProjectId()));
        }
        details.setRecipientList(projectInfo.getRecipientList());
        details.setCreated(projectInfo.getCreated().getTime());
        details.setUpdated(projectInfo.getUpdated().getTime());
        details.setCreatedBy(projectInfo.getCreatedBy());
        details.setTeamId(projectInfo.getTeamId());
        details.setProjectDescription(projectInfo.getProjectDescription());
        details.setPurposeOfUse(projectInfo.getPurposeOfUse());
        return details;
    }

    private ProjectSummary getProjectSummary(String customerSpace, ProjectInfo projectInfo,
                                             DataReport.BasicStats basicStats, Boolean includeSources) {
        ProjectSummary summary = new ProjectSummary();
        summary.setProjectId(projectInfo.getProjectId());
        summary.setProjectDisplayName(projectInfo.getProjectDisplayName());
        summary.setArchived(projectInfo.getDeleted());
        if(includeSources) {
            summary.setSources(sourceService.getSourceList(customerSpace, projectInfo.getProjectId()));
            summary.setTotalSourceCount(sourceService.getSourceCount(customerSpace, projectInfo.getProjectId()));
        }
        summary.setRecipientList(projectInfo.getRecipientList());
        summary.setBasicStats(basicStats);
        summary.setCreated(projectInfo.getCreated().getTime());
        summary.setUpdated(projectInfo.getUpdated().getTime());
        summary.setCreatedBy(projectInfo.getCreatedBy());
        summary.setProjectDescription(projectInfo.getProjectDescription());
        summary.setPurposeOfUse(projectInfo.getPurposeOfUse());
        return summary;
    }

    private Project generateProjectObject(String projectId, String displayName,
                                          Project.ProjectType projectType, String user, String rootPath,
                                          PurposeOfUse purposeOfUse, String description) {
        Project project = new Project();
        project.setCreatedBy(user);
        project.setProjectDisplayName(displayName);
        project.setProjectId(projectId);
        project.setTenant(MultiTenantContext.getTenant());
        project.setDeleted(Boolean.FALSE);
        project.setProjectType(projectType);
        project.setRootPath(rootPath);
        project.setRecipientList(Collections.singletonList(user));
        project.setPurposeOfUse(purposeOfUse);
        project.setProjectDescription(description);
        return project;
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

    private ProjectInfo getProjectInfoByProjectIdWithRetry(String projectId) {
        int retry = 0;
        ProjectInfo project = projectEntityMgr.findProjectInfoByProjectId(projectId);
        while (project == null && retry < MAX_RETRY) {
            try {
                Thread.sleep(1000L + retry * 1000L);
            } catch (InterruptedException e) {
                return null;
            }
            project = projectEntityMgr.findProjectInfoByProjectId(projectId);
            retry++;
        }
        return project;
    }
}
