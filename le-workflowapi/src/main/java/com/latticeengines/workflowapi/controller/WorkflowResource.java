package com.latticeengines.workflowapi.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.network.exposed.workflowapi.WorkflowInterface;
import com.latticeengines.workflow.exposed.service.WorkflowService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "workflow", description = "REST resource for workflows")
@RestController
@RequestMapping("/workflows/customerspaces/{customerSpace}")
public class WorkflowResource implements WorkflowInterface {

    private static final Logger log = LoggerFactory.getLogger(WorkflowResource.class);

    @Autowired
    private WorkflowService workflowService;

    @RequestMapping(value = "/job/{workflowId}/stop", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Stop an executing workflow")
    @Override
    public void stopWorkflow(@PathVariable String customerSpace, @PathVariable String workflowId) {
        workflowService.stop(customerSpace, new WorkflowExecutionId(Long.valueOf(workflowId)));
    }

    @RequestMapping(value = "/jobs", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of workflow executions given list of job Ids")
    @Override
    public List<Job> getWorkflowJobs( //
            @PathVariable String customerSpace, //
            @RequestParam(value = "jobIds", required = false) List<String> jobIds, //
            @RequestParam(value = "types", required = false) List<String> types, //
            @RequestParam(value = "includeDetails", required = false) Boolean includeDetails, //
            @RequestParam(value = "hasParentId", required = false) Boolean hasParentId) {
        return null;
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update workflow jobs' parent job Id")
    @Override
    public Job updateParentJobId( //
            @PathVariable String customerSpace, //
            @RequestParam(value = "jobIds", required = true) List<String> jobIds, //
            @RequestParam(value = "parentId", required = true) String parentJobId) {
        return null;
    }

}