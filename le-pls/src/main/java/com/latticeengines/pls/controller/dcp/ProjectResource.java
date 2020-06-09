package com.latticeengines.pls.controller.dcp;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectRequest;
import com.latticeengines.domain.exposed.dcp.ProjectSummary;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.UIActionException;
import com.latticeengines.domain.exposed.pls.frontend.Status;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
import com.latticeengines.domain.exposed.pls.frontend.View;
import com.latticeengines.pls.service.dcp.ProjectService;
import com.latticeengines.pls.service.impl.GraphDependencyToUIActionUtil;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Projects")
@RestController
@RequestMapping("/projects")
public class ProjectResource {

    private static final Logger log = LoggerFactory.getLogger(ProjectResource.class);

    @Inject
    private ProjectService projectService;

    @Inject
    private GraphDependencyToUIActionUtil graphDependencyToUIActionUtil;

    @PostMapping
    @ResponseBody
    @ApiOperation("Create new project")
    @PreAuthorize("hasRole('Edit_DCP_Projects')")
    public ProjectDetails createProject(@RequestBody ProjectRequest projectRequest) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }

        try {
            return projectService.createProject(customerSpace.toString(), projectRequest, MultiTenantContext.getEmailAddress());
        } catch (LedpException e) {
            log.error("Failed to create project: " + e.getMessage());
            UIAction action = graphDependencyToUIActionUtil.generateUIAction("", View.Banner,
                    Status.Error, e.getMessage());
            throw new UIActionException(action, e.getCode());
        }
    }

    @GetMapping("/list")
    @ResponseBody
    @ApiOperation("Get all projects")
    @PreAuthorize("hasRole('View_DCP_Projects')")
    List<ProjectSummary> getAllProjects() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }

        try {
            return projectService.getAllProjects(customerSpace.toString());
        } catch (LedpException e) {
            log.error("Failed to get all projects: " + e.getMessage());
            UIAction action = graphDependencyToUIActionUtil.generateUIAction("", View.Banner,
                    Status.Error, e.getMessage());
            throw new UIActionException(action, e.getCode());
        }
    }

    @GetMapping("/projectId/{projectId}")
    @ResponseBody
    @ApiOperation("Get project by projectId")
    @PreAuthorize("hasRole('View_DCP_Projects')")
    ProjectDetails getProjectByProjectId(@PathVariable String projectId) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }

        try {
            return projectService.getProjectByProjectId(customerSpace.toString(), projectId);
        } catch (LedpException e) {
            log.error("Failed to get project by projectId: " + e.getMessage());
            UIAction action = graphDependencyToUIActionUtil.generateUIAction("", View.Banner,
                    Status.Error, e.getMessage());
            throw new UIActionException(action, e.getCode());
        }
    }

    @DeleteMapping("/{projectId}")
    @ResponseBody
    @ApiOperation("Archive project by projectId")
    @PreAuthorize("hasRole('Edit_DCP_Projects')")
    void archiveProject(@PathVariable String projectId) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }

        try {
            projectService.deleteProject(customerSpace.toString(), projectId);
        } catch (LedpException e) {
            log.error("Failed to archive project by projectId: " + e.getMessage());
            UIAction action = graphDependencyToUIActionUtil.generateUIAction("", View.Banner,
                    Status.Error, e.getMessage());
            throw new UIActionException(action, e.getCode());
        }
    }

}
