package com.latticeengines.pls.service.impl.dcp;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.latticeengines.auth.exposed.util.TeamUtils;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectRequest;
import com.latticeengines.domain.exposed.dcp.ProjectSummary;
import com.latticeengines.domain.exposed.pls.GlobalTeamData;
import com.latticeengines.pls.service.TeamWrapperService;
import com.latticeengines.pls.service.dcp.ProjectService;
import com.latticeengines.proxy.exposed.dcp.ProjectProxy;


@Component("projectService")
public class ProjectServiceImpl implements ProjectService {

    private static final Logger log = LoggerFactory.getLogger(ProjectServiceImpl.class);
    private static final int DEFAULT_PAGE_SIZE = 20;

    private static final List<String> TEAM_REGARDLESS_ROLES = Arrays.asList( //
            "SUPER_ADMIN", "INTERNAL_ADMIN");

    @Inject
    private ProjectProxy projectProxy;

    @Inject
    private TeamWrapperService teamWrapperService;

    @Inject
    private BatonService batonService;

    @Override
    public ProjectDetails createProject(String customerSpace, ProjectRequest projectRequest, String user) {
        if(projectRequest.getTeamId() == null || projectRequest.getTeamId().isEmpty()){
            GlobalTeamData globalTeamData = new GlobalTeamData();
            globalTeamData.setTeamName(projectRequest.getDisplayName());
            globalTeamData.setTeamMembers(Sets.newHashSet(user));
            String teamId = teamWrapperService.createTeam(user, globalTeamData);
            projectRequest.setTeamId(teamId);
        }

        return projectProxy.createDCPProject(customerSpace, projectRequest, user);
    }

    @Override
    public List<ProjectSummary> getAllProjects(String customerSpace, Boolean includeSources) {
        return projectProxy.getAllDCPProject(customerSpace, includeSources, 0, DEFAULT_PAGE_SIZE);
    }

    @Override
    public ProjectDetails getProjectByProjectId(String customerSpace, String projectId, Boolean includeSources) {
        ProjectDetails projectDetails =  projectProxy.getDCPProjectByProjectId(customerSpace, projectId, includeSources);
        if(skipTeamCheck() || TeamUtils.isMyTeam(projectDetails.getTeamId())){
            return projectDetails;
        } else {
            throw new AccessDeniedException("Access denied.");
        }
    }

    @Override
    public void deleteProject(String customerSpace, String projectId) {
        ProjectDetails projectDetails =  projectProxy.getDCPProjectByProjectId(customerSpace, projectId, false);
        if(skipTeamCheck() || TeamUtils.isMyTeam(projectDetails.getTeamId())){
            projectProxy.deleteProject(customerSpace, projectId);
        } else {
            throw new AccessDeniedException("Access denied.");
        }
    }

    @Override
    public GrantDropBoxAccessResponse getDropFolderAccessByProjectId(String customerSpace, String projectId) {
        ProjectDetails projectDetails =  projectProxy.getDCPProjectByProjectId(customerSpace, projectId, false);
        if(skipTeamCheck() || TeamUtils.isMyTeam(projectDetails.getTeamId())){
            return projectProxy.getDropFolderAccessByProjectId(customerSpace, projectId);
        } else {
            throw new AccessDeniedException("Access denied.");
        }
    }

    private boolean skipTeamCheck() {
        boolean teamFeatureEnabled = batonService.isEnabled(MultiTenantContext.getCustomerSpace(), LatticeFeatureFlag.TEAM_FEATURE);
        return !teamFeatureEnabled || TEAM_REGARDLESS_ROLES.contains(MultiTenantContext.getSession().getAccessLevel());
    }

}
