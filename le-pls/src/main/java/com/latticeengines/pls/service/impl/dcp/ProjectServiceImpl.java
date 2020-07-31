package com.latticeengines.pls.service.impl.dcp;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectRequest;
import com.latticeengines.domain.exposed.dcp.ProjectSummary;
import com.latticeengines.domain.exposed.pls.GlobalTeamData;
import com.latticeengines.domain.exposed.security.Session;
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

    @Override
    public ProjectDetails createProject(String customerSpace, ProjectRequest projectRequest, String user) {
        ProjectDetails project = projectProxy.createDCPProject(customerSpace, projectRequest, user);
        if(project != null){
            GlobalTeamData globalTeamData = new GlobalTeamData();
            globalTeamData.setTeamName(projectRequest.getDisplayName());
            globalTeamData.setTeamMembers(Sets.newHashSet(user));
            String teamId = teamWrapperService.createTeam(user, globalTeamData);
            projectProxy.updateTeamId(customerSpace, project.getProjectId(), teamId);
        }
        return project;
    }

    @Override
    public List<ProjectSummary> getAllProjects(String customerSpace, Boolean includeSources) {
        return projectProxy.getAllDCPProject(customerSpace, includeSources, 0, DEFAULT_PAGE_SIZE, getTeamIds());
    }

    @Override
    public List<ProjectSummary> getAllProjects(String customerSpace, Boolean includeSources, int pageIndex, int pageSize) {
        Preconditions.checkArgument(pageIndex > 0);
        Preconditions.checkArgument(pageSize > 0);
        return projectProxy.getAllDCPProject(customerSpace, includeSources, pageIndex - 1, pageSize, getTeamIds());
    }

    @Override
    public ProjectDetails getProjectByProjectId(String customerSpace, String projectId, Boolean includeSources) {
        return projectProxy.getDCPProjectByProjectId(customerSpace, projectId, includeSources, getTeamIds());
    }

    @Override
    public void deleteProject(String customerSpace, String projectId) {
        projectProxy.deleteProject(customerSpace, projectId, getTeamIds());
    }

    @Override
    public GrantDropBoxAccessResponse getDropFolderAccessByProjectId(String customerSpace, String projectId) {
        return projectProxy.getDropFolderAccessByProjectId(customerSpace, projectId, getTeamIds());
    }

    private List<String> getTeamIds() {
        if(TEAM_REGARDLESS_ROLES.contains(MultiTenantContext.getSession().getAccessLevel())){
            return null;
        } else {
            Session session = MultiTenantContext.getSession();
            if (session != null) {
                return session.getTeamIds();
            } else {
                return Collections.emptyList();
            }
        }
    }

}
