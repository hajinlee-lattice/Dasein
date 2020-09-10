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
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectRequest;
import com.latticeengines.domain.exposed.dcp.ProjectSummary;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.Status;
import com.latticeengines.domain.exposed.exception.UIAction;
import com.latticeengines.domain.exposed.exception.UIActionException;
import com.latticeengines.domain.exposed.exception.UIActionUtils;
import com.latticeengines.domain.exposed.exception.View;
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
            UIAction action = UIActionUtils.generateUIAction("", View.Banner,
                    Status.Error, e.getMessage());
            throw new UIActionException(action, e.getCode());
        }
    }

    @GetMapping("/list")
    @ResponseBody
    @ApiOperation("Get all projects")
    @PreAuthorize("hasRole('View_DCP_Projects')")
    List<ProjectSummary> getAllProjects(@RequestParam(required = false, defaultValue = "false") Boolean includeSources,
                                        @RequestParam(required = false, defaultValue = "false") Boolean includeArchived,
                                        @RequestParam(required = false, defaultValue = "1") int pageIndex,
                                        @RequestParam(required = false, defaultValue = "20") int pageSize) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }

        try {
            return projectService.getAllProjects(customerSpace.toString(), includeSources, includeArchived, pageIndex, pageSize);
        } catch (LedpException e) {
            log.error("Failed to get all projects: " + e.getMessage());
            UIAction action = UIActionUtils.generateUIAction("", View.Banner,
                    Status.Error, e.getMessage());
            throw new UIActionException(action, e.getCode());
        }
    }

    @GetMapping("/projectId/{projectId}")
    @ResponseBody
    @ApiOperation("Get project by projectId")
    @PreAuthorize("hasRole('View_DCP_Projects')")
    ProjectDetails getProjectByProjectId(@PathVariable String projectId,
                                         @RequestParam(required = false, defaultValue = "true") Boolean includeSources) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }

        try {
            return projectService.getProjectByProjectId(customerSpace.toString(), projectId, includeSources);
        } catch (LedpException e) {
            log.error("Failed to get project by projectId: " + e.getMessage());
            UIAction action = UIActionUtils.generateUIAction("", View.Banner,
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
            UIAction action = UIActionUtils.generateUIAction("", View.Banner,
                    Status.Error, e.getMessage());
            throw new UIActionException(action, e.getCode());
        }
    }

    @GetMapping("/projectId/{projectId}/dropFolderAccess")
    @ResponseBody
    @ApiOperation("Get project by projectId")
    @PreAuthorize("hasRole('View_DCP_Projects')")
    GrantDropBoxAccessResponse getDropFolderAccessByProjectId(@PathVariable String projectId) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }

        try {
            return projectService.getDropFolderAccessByProjectId(customerSpace.toString(), projectId);
        } catch (LedpException e) {
            log.error("Failed to get dropFolderAccess by projectId: " + e.getMessage());
            UIAction action = UIActionUtils.generateUIAction("", View.Banner,
                    Status.Error, e.getMessage());
            throw new UIActionException(action, e.getCode());
        }
    }

    @PutMapping("/projectId/{projectId}/description")
    @ResponseBody
    @ApiOperation("update project description")
    @PreAuthorize("hasRole('Edit_DCP_Projects')")
    void updateDescription(@PathVariable String projectId, @RequestBody String description) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        try {
            projectService.updateDescription(customerSpace.toString(), projectId, description);
        } catch (Exception e) {
            log.error("Failed to update project description by projectId: " + e.getMessage());
            throw UIActionUtils.handleException(e);
        }
    }
}
