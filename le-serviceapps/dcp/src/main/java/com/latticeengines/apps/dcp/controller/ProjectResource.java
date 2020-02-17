package com.latticeengines.apps.dcp.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.dcp.service.ProjectService;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.exception.LedpException;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "project", description = "REST resource for project")
@RestController
@RequestMapping(value = "/customerspaces/{customerSpace}/project")
public class ProjectResource {

    @Inject
    private ProjectService projectService;

    @PostMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Create an Project")
    public ResponseDocument<ProjectDetails> createProject(@PathVariable String customerSpace,
                                                          @RequestParam(required = false) String projectId,
                                                          @RequestParam String displayName,
                                                          @RequestParam String user,
                                                          @RequestBody Project.ProjectType projectType) {
        try {
            ProjectDetails result;
            if(projectId == null) {
                result = projectService.createProject(customerSpace, displayName, projectType, user);
            } else {
                result = projectService.createProject(customerSpace, projectId, displayName, projectType, user);
            }
            return ResponseDocument.successResponse(result);
        } catch (LedpException e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @GetMapping(value = "/list")
    @ResponseBody
    @ApiOperation(value = "Get all projects")
    public List<Project> getAllProject(@PathVariable String customerSpace) {
        return projectService.getAllProject(customerSpace);
    }

    @GetMapping(value = "/{projectId}")
    @ResponseBody
    @ApiOperation(value = "Get project by projectId")
    public ProjectDetails getProjectByProjectId(@PathVariable String customerSpace, @PathVariable String projectId) {
        return projectService.getProjectByProjectId(customerSpace, projectId);
    }

    @DeleteMapping(value = "/{projectId}")
    @ResponseBody
    @ApiOperation(value = "Delete project by projectId")
    public Boolean deleteProject(@PathVariable String customerSpace, @PathVariable String projectId) {
        return projectService.deleteProject(customerSpace, projectId);
    }
}
