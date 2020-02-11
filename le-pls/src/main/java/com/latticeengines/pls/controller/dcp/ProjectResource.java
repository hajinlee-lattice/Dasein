package com.latticeengines.pls.controller.dcp;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.DCPProject;
import com.latticeengines.domain.exposed.dcp.DCPProjectDetails;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.UIActionException;
import com.latticeengines.domain.exposed.pls.frontend.Status;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
import com.latticeengines.domain.exposed.pls.frontend.View;
import com.latticeengines.pls.service.DCPProjectService;
import com.latticeengines.pls.service.impl.GraphDependencyToUIActionUtil;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dcp resource", description = "REST resource for dcp")
@RestController
@RequestMapping("/dcp/dcpproject")
public class ProjectResource {

    private static final Logger log = LoggerFactory.getLogger(ProjectResource.class);

    @Inject
    private DCPProjectService dcpProjectService;

    @Inject
    private GraphDependencyToUIActionUtil graphDependencyToUIActionUtil;

    @PostMapping(value = "")
    @ResponseBody
    @ApiOperation("create new DCP project")
    @PreAuthorize("hasRole('Edit_DCP_Projects')")
    public DCPProjectDetails createDCPProject(@RequestParam String displayName,
                                                  @RequestParam(required = false) String projectId,
                                                  @RequestParam DCPProject.ProjectType projectType) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }

        try {
            return dcpProjectService.createDCPProject(customerSpace.toString(), projectId, displayName, projectType, MultiTenantContext.getEmailAddress());
        } catch (LedpException e) {
            log.error("Failed to create DCP project: " + e.getMessage());
            UIAction action = graphDependencyToUIActionUtil.generateUIAction("", View.Banner,
                    Status.Error, e.getMessage());
            throw new UIActionException(action, e.getCode());
        }
    }

    @GetMapping(value = "/list")
    @ResponseBody
    @ApiOperation("get all DCP project")
    @PreAuthorize("hasRole('View_DCP_Projects')")
    List<DCPProject> getAllDCPProject() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }

        try {
            return dcpProjectService.getAllDCPProject(customerSpace.toString());
        } catch (LedpException e) {
            log.error("Failed to get all DCP project: " + e.getMessage());
            UIAction action = graphDependencyToUIActionUtil.generateUIAction("", View.Banner,
                    Status.Error, e.getMessage());
            throw new UIActionException(action, e.getCode());
        }
    }

    @GetMapping(value = "")
    @ResponseBody
    @ApiOperation("get DCP project by projectId")
    @PreAuthorize("hasRole('View_DCP_Projects')")
    DCPProjectDetails getDCPProjectByProjectId(@RequestParam String projectId) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }

        try {
            return dcpProjectService.getDCPProjectByProjectId(customerSpace.toString(), projectId);
        } catch (LedpException e) {
            log.error("Failed to get DCP project by projectId: " + e.getMessage());
            UIAction action = graphDependencyToUIActionUtil.generateUIAction("", View.Banner,
                    Status.Error, e.getMessage());
            throw new UIActionException(action, e.getCode());
        }
    }

    @DeleteMapping(value = "")
    @ResponseBody
    @ApiOperation("delete DCP project by projectId")
    @PreAuthorize("hasRole('Edit_DCP_Projects')")
    void deleteProject(@RequestParam String projectId) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }

        try {
            dcpProjectService.deleteProject(customerSpace.toString(), projectId);
        } catch (LedpException e) {
            log.error("Failed to delete DCP project by projectId: " + e.getMessage());
            UIAction action = graphDependencyToUIActionUtil.generateUIAction("", View.Banner,
                    Status.Error, e.getMessage());
            throw new UIActionException(action, e.getCode());
        }
    }

}
