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
import com.latticeengines.common.exposed.annotation.UseReaderConnection;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectRequest;
import com.latticeengines.domain.exposed.dcp.ProjectSummary;
import com.latticeengines.domain.exposed.exception.LedpException;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "project", description = "REST resource for project")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/project")
public class ProjectResource {

    @Inject
    private ProjectService projectService;

    @PostMapping
    @ResponseBody
    @ApiOperation(value = "Create an Project")
    public ResponseDocument<ProjectDetails> createProject(@PathVariable String customerSpace,
                                                          @RequestParam String user,
                                                          @RequestBody ProjectRequest projectRequest) {
        try {
            ProjectDetails result;
            if(projectRequest.getProjectId() == null) {
                result = projectService.createProject(customerSpace, projectRequest.getDisplayName(),
                        projectRequest.getProjectType(), user);
            } else {
                result = projectService.createProject(customerSpace, projectRequest.getProjectId(),
                        projectRequest.getDisplayName(), projectRequest.getProjectType(), user);
            }
            return ResponseDocument.successResponse(result);
        } catch (LedpException e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @GetMapping("/list")
    @ResponseBody
    @ApiOperation(value = "Get all projects")
    @UseReaderConnection
    public List<ProjectSummary> getAllProject(@PathVariable String customerSpace,
                                              @RequestParam(defaultValue = "false") Boolean includeSources,
                                              @RequestParam(defaultValue = "0") int pageIndex,
                                              @RequestParam(defaultValue = "20") int pageSize) {
        return projectService.getAllProject(customerSpace, includeSources, pageIndex, pageSize);
    }

    @GetMapping("/count")
    @ResponseBody
    @ApiOperation(value = "Get all projects count")
    @UseReaderConnection
    public Long getAllProjectCount(@PathVariable String customerSpace) {
        return projectService.getProjectsCount(customerSpace);
    }

    @GetMapping("/projectId/{projectId}")
    @ResponseBody
    @ApiOperation(value = "Get project by projectId")
    @UseReaderConnection
    public ProjectDetails getProjectByProjectId(@PathVariable String customerSpace, @PathVariable String projectId,
                                                @RequestParam(defaultValue = "true") Boolean includeSources) {
        return projectService.getProjectDetailByProjectId(customerSpace, projectId, includeSources);
    }

    @DeleteMapping("/{projectId}")
    @ResponseBody
    @ApiOperation(value = "Delete project by projectId")
    public Boolean deleteProject(@PathVariable String customerSpace, @PathVariable String projectId) {
        return projectService.deleteProject(customerSpace, projectId);
    }

    @GetMapping("/projectId/{projectId}/dropFolderAccess")
    @ResponseBody
    @ApiOperation(value = "Get dropFolderAccess by projectId")
    @UseReaderConnection
    public GrantDropBoxAccessResponse getDropFolderAccessByProjectId(@PathVariable String customerSpace, @PathVariable String projectId) {
        return projectService.getDropFolderAccessByProjectId(customerSpace, projectId);
    }
}
