package com.latticeengines.workflowapi.controller;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflowapi.service.WorkflowContainerService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "workflow", description = "REST resource for workflows")
@RestController
@RequestMapping("/workflows")
public class WorkflowResource {

    private static final Log log = LogFactory.getLog(WorkflowResource.class);

    @Autowired
    private WorkflowContainerService workflowContainerService;

    @Autowired
    private WorkflowService workflowService;

    @RequestMapping(value = "/", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a workflow execution in a Yarn container")
    public AppSubmission submitWorkflowExecution(@RequestBody WorkflowConfiguration workflowConfig) {
        return new AppSubmission(Arrays.<ApplicationId>asList(new ApplicationId[] { workflowContainerService.submitWorkFlow(workflowConfig) }));
    }

    @RequestMapping(value = "/yarnapps/id/{applicationId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get workflowId from the applicationId of a workflow execution in a Yarn container")
    public String getWorkflowId(@PathVariable String applicationId) {
        return getWorkflowIdFromAppId(applicationId);
    }

    @RequestMapping(value = "/status/{workflowId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get status about a submitted workflow")
    public WorkflowStatus getWorkflowStatus(@PathVariable String workflowId) {
        return workflowService.getStatus(new WorkflowExecutionId(Long.valueOf(workflowId)));
    }

    @RequestMapping(value = "/yarnapps/status/{applicationId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get status about a submitted workflow from a YARN application id")
    public WorkflowStatus getWorkflowStatusFromApplicationId(@PathVariable String applicationId) {
        String workflowId = getWorkflowIdFromAppId(applicationId);
        return workflowService.getStatus(new WorkflowExecutionId(Long.valueOf(workflowId)));
    }

    private String getWorkflowIdFromAppId(String applicationId) {
        log.info("getWorkflowId for applicationId:" + applicationId);
        WorkflowExecutionId workflowId = workflowContainerService.getWorkflowId(YarnUtils.appIdFromString(applicationId));
        return String.valueOf(workflowId.getId());
    }

}
