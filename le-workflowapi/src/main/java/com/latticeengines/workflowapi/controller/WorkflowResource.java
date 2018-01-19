package com.latticeengines.workflowapi.controller;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.workflowapi.service.WorkflowJobService;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "workflow", description = "REST resource for workflows")
@RestController
@RequestMapping("/workflows")
public class WorkflowResource {

    private static final Logger log = LoggerFactory.getLogger(WorkflowResource.class);

    @Autowired
    private WorkflowJobService workflowJobService;

    @RequestMapping(value = "/job/{workflowId}/stop", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Stop an executing workflow")
    public void stopWorkflowExecution(@PathVariable String workflowId,
                                      @RequestParam(required = false) String customerSpace) {
        workflowJobService.stopWorkflow(customerSpace, Long.valueOf(workflowId));
    }

    @RequestMapping(value = "/job/{workflowId}/restart", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Restart a previous workflow execution")
    public AppSubmission restartWorkflowExecution(@PathVariable String workflowId,
                                                  @RequestParam(required = false) String customerSpace) {
        Long wfId = Long.valueOf(workflowId);
        WorkflowStatus status = workflowJobService.getWorkflowStatus(customerSpace, wfId);

        if (status == null) {
            throw new LedpException(LedpCode.LEDP_28017, new String[] { workflowId });
        } else if (!WorkflowStatus.TERMINAL_BATCH_STATUS.contains(status.getStatus())) {
            throw new LedpException(LedpCode.LEDP_28018, new String[] { workflowId, status.getStatus().name() });
        }

        Job job = workflowJobService.getJob(customerSpace, wfId, false);
        WorkflowConfiguration workflowConfig = new WorkflowConfiguration();
        workflowConfig.setWorkflowName(status.getWorkflowName());
        workflowConfig.setRestart(true);
        workflowConfig.setWorkflowIdToRestart(new WorkflowExecutionId(wfId));
        workflowConfig.setCustomerSpace(status.getCustomerSpace());
        workflowConfig.setInputProperties(job.getInputs());
        workflowConfig.setUserId(job.getUser());

        return new AppSubmission(workflowJobService.submitWorkFlow(customerSpace, workflowConfig));
    }

    @RequestMapping(value = "/job/{workflowId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get a workflow execution")
    public Job getWorkflowExecution(@PathVariable String workflowId,
                                    @RequestParam(required = false) String customerSpace) {
        return workflowJobService.getJob(customerSpace, Long.valueOf(workflowId), true);
    }

    @RequestMapping(value = "/jobs", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get list of workflow jobs by given list of job Ids or job types.")
    public List<Job> getJobs(@RequestParam(value = "jobId", required = false) List<String> jobIds,
                             @RequestParam(value = "type", required = false) List<String> types,
                             @RequestParam(value = "includeDetails", required = false) Boolean includeDetails,
                             @RequestParam(required = false) String customerSpace) {
        Optional<List<String>> optionalJobIds = Optional.ofNullable(jobIds);
        Optional<List<String>> optionalTypes = Optional.ofNullable(types);
        Optional<Boolean> optionalIncludeDetails = Optional.ofNullable(includeDetails);

        if (optionalJobIds.isPresent()) {
            List<Long> workflowIds = optionalJobIds.get().stream().map(Long::valueOf).collect(Collectors.toList());
            return workflowJobService.getJobs(customerSpace, workflowIds, optionalTypes.orElse(null),
                    optionalIncludeDetails.orElse(true), false, -1L);
        } else if (optionalTypes.isPresent()) {
            return workflowJobService.getJobs(customerSpace, null, optionalTypes.get(),
                    optionalIncludeDetails.orElse(true), false, -1L);
        } else {
            return workflowJobService.getJobs(customerSpace, optionalIncludeDetails.orElse(true));
        }
    }

    @RequestMapping(value = "/jobs", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ApiOperation(value = "Update workflow jobs' parent job Id")
    public void updateParentJobId(@RequestParam(value = "jobId", required = true) List<String> jobIds,
                                  @RequestParam(value = "parentId", required = true) String parentJobId,
                                  @RequestParam(required = false) String customerSpace) {
        workflowJobService.updateParentJobId(customerSpace,
                jobIds.stream().map(Long::valueOf).collect(Collectors.toList()), Long.valueOf(parentJobId));
    }

    @RequestMapping(value = "/jobs", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Create a workflow execution in a Yarn container")
    public AppSubmission submitWorkflowExecution(@RequestBody WorkflowConfiguration config,
                                                 @RequestParam(required = false) String customerSpace) {
        return new AppSubmission(workflowJobService.submitWorkFlow(customerSpace, config));
    }

    @RequestMapping(value = "/awsJobs", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Create a workflow execution in a AWS container")
    public String submitAWSWorkflowExecution(@RequestBody WorkflowConfiguration workflowConfig,
                                             @RequestParam(required = false) String customerSpace) {
        return workflowJobService.submitAwsWorkflow(customerSpace, workflowConfig);
    }

    @RequestMapping(value = "/yarnapps/id/{applicationId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get workflowId from the applicationId of a workflow execution in a Yarn container")
    public WorkflowExecutionId getWorkflowId(@PathVariable String applicationId,
                                             @RequestParam(required = false) String customerSpace) {
        return workflowJobService.getWorkflowExecutionIdByApplicationId(customerSpace, applicationId);
    }

    @RequestMapping(value = "/status/{workflowId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get status about a submitted workflow")
    public WorkflowStatus getWorkflowStatus(@PathVariable String workflowId,
                                            @RequestParam(required = false) String customerSpace) {
        return workflowJobService.getWorkflowStatus(customerSpace, Long.valueOf(workflowId));
    }

    @RequestMapping(value = "/yarnapps/job/{applicationId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get status about a submitted workflow from a YARN application id")
    public Job getWorkflowJobFromApplicationId(@PathVariable String applicationId,
                                               @RequestParam(required = false) String customerSpace) {
        return workflowJobService.getJobByApplicationId(customerSpace, applicationId, true);
    }
}
