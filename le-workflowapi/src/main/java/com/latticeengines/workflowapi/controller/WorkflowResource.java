package com.latticeengines.workflowapi.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

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
}