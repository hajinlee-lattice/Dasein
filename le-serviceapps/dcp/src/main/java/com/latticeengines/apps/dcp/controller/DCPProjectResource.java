package com.latticeengines.apps.dcp.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.dcp.service.DCPProjectService;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dcp.DCPProject;
import com.latticeengines.domain.exposed.dcp.DCPProjectDetails;
import com.latticeengines.domain.exposed.exception.LedpException;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dcpproject", description = "REST resource for dcp project")
@RestController
@RequestMapping(value = "/customerspaces/{customerSpace}/dcpproject")
public class DCPProjectResource {

    @Inject
    private DCPProjectService dcpProjectService;

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create an DCP Project")
    public ResponseDocument<DCPProjectDetails> createDCPProject(@PathVariable String customerSpace,
                                                      @RequestParam(required = false) String projectId,
                                                      @RequestParam String displayName,
                                                      @RequestParam String user,
                                                      @RequestBody DCPProject.ProjectType projectType) {
        try {
            DCPProjectDetails result;
            if(projectId == null) {
                result = dcpProjectService.createDCPProject(customerSpace, displayName, projectType, user);
            } else {
                result = dcpProjectService.createDCPProject(customerSpace, projectId, displayName, projectType, user);
            }
            return ResponseDocument.successResponse(result);
        } catch (LedpException e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "/list", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all DCP projects")
    public List<DCPProject> getAllDCPProject(@PathVariable String customerSpace) {
        return dcpProjectService.getAllDCPProject(customerSpace);
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get DCP project by projectId")
    public DCPProjectDetails getDCPProjectByProjectId(@PathVariable String customerSpace, @RequestParam String projectId) {
        return dcpProjectService.getDCPProjectByProjectId(customerSpace, projectId);
    }

    @RequestMapping(value = "", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete DCP project by projectId")
    public void deleteProject(@PathVariable String customerSpace, @RequestParam String projectId) {
        dcpProjectService.deleteProject(customerSpace, projectId);
    }
}
