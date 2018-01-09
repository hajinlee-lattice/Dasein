package com.latticeengines.workflowapi.controller;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.network.exposed.workflowapi.WorkflowInterface;
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
public class WorkflowResource implements WorkflowInterface {

    private static final Logger log = LoggerFactory.getLogger(WorkflowResource.class);

    @Autowired
//    private WorkflowService workflowService;
    private WorkflowJobService workflowJobService;

    @RequestMapping(value = "/customerspaces/{customerSpace}/job/{workflowId}/stop", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Stop an executing workflow")
    @Override
    public void stopWorkflow(@PathVariable String customerSpace, @PathVariable String workflowId) {
//        workflowService.stop(customerSpace, new WorkflowExecutionId(Long.valueOf(workflowId)));
        workflowJobService.stopWorkflow(customerSpace, Long.valueOf(workflowId));
    }

    @RequestMapping(value = "/customerspaces/{customerSpace}/jobs", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of workflow executions given list of job Ids")
    @Override
    public List<Job> getWorkflowJobs(@PathVariable String customerSpace,
                                     @RequestParam(value = "jobId", required = false) List<String> jobIds,
                                     @RequestParam(value = "type", required = false) List<String> types,
                                     @RequestParam(value = "includeDetails", required = false) Boolean includeDetails) {
        return workflowJobService.getJobs(customerSpace,
                jobIds.stream().map(Long::valueOf).collect(Collectors.toList()),
                types, includeDetails, false, -1L);
    }

    @RequestMapping(value = "/customerspaces/{customerSpace}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update workflow jobs' parent job Id")
    @Override
    public List<Job> updateParentJobId(@PathVariable String customerSpace,
                                       @RequestParam(value = "jobId", required = true) List<String> jobIds,
                                       @RequestParam(value = "parentId", required = true) String parentJobId) {
        return workflowJobService.updateParentJobId(customerSpace,
                jobIds.stream().map(Long::valueOf).collect(Collectors.toList()),
                Long.valueOf(parentJobId));
    }

    @RequestMapping(value = "/", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a workflow execution in a Yarn container")
    @Override
    public AppSubmission submitWorkflowExecution(@RequestBody WorkflowConfiguration config) {
        return new AppSubmission(workflowJobService.submitWorkFlow(config));
    }

    @RequestMapping(value = "/aws", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a workflow execution in a AWS container")
    @Override
    public String submitAWSWorkflowExecution(@RequestBody WorkflowConfiguration workflowConfig) {
        return workflowJobService.submitAwsWorkflow(workflowConfig);
    }

    @RequestMapping(value = "/job/{workflowId}/restart", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Restart a previous workflow execution")
    @Override
    public AppSubmission restartWorkflowExecution(@PathVariable Long workflowId) {
        //        WorkflowExecutionId workflowExecutionId = new WorkflowExecutionId(workflowId);
//        WorkflowStatus status = workflowService.getStatus(workflowExecutionId);
        WorkflowStatus status = workflowJobService.getWorkflowStatus(workflowId);

        if (status == null) {
            throw new LedpException(LedpCode.LEDP_28017, new String[] { String.valueOf(workflowId) });
        } else if (!WorkflowStatus.TERMINAL_BATCH_STATUS.contains(status.getStatus())) {
            throw new LedpException(LedpCode.LEDP_28018,
                    new String[] { String.valueOf(workflowId), status.getStatus().name() });
        }
//        WorkflowJob workflowJob = workflowService.getJob(workflowId);
        Job job = workflowJobService.getJob(workflowId, false);
        WorkflowConfiguration workflowConfig = new WorkflowConfiguration();
        workflowConfig.setWorkflowName(status.getWorkflowName());
        workflowConfig.setRestart(true);
        workflowConfig.setWorkflowIdToRestart(new WorkflowExecutionId(workflowId));
        workflowConfig.setCustomerSpace(status.getCustomerSpace());
        workflowConfig.setInputProperties(job.getInputs());
        workflowConfig.setUserId(job.getUser());

//        return new AppSubmission(Arrays.<ApplicationId> asList(
//                new ApplicationId[] { workflowContainerService.submitWorkFlow(workflowConfig) }));
        return new AppSubmission(workflowJobService.submitWorkFlow(workflowConfig));
    }

    @RequestMapping(value = "/yarnapps/id/{applicationId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get workflowId from the applicationId of a workflow execution in a Yarn container")
    @Override
    public WorkflowExecutionId getWorkflowId(@PathVariable String applicationId) {
        //        return workflowContainerService.getWorkflowId(ConverterUtils.toApplicationId(applicationId));
        return workflowJobService.getWorkflowExecutionIdByApplicationId(applicationId);
    }

    @RequestMapping(value = "/status/{workflowId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get status about a submitted workflow")
    @Override
    public WorkflowStatus getWorkflowStatus(@PathVariable String workflowId) {
        return workflowJobService.getWorkflowStatus(Long.valueOf(workflowId));
    }

    @RequestMapping(value = "/yarnapps/jobs/{applicationId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get status about a submitted workflow from a YARN application id")
    @Override
    public Job getWorkflowJobFromApplicationId(@PathVariable String applicationId) {
        return workflowJobService.getJobByApplicationId(applicationId);
    }

    @RequestMapping(value = "/job/{workflowId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a workflow execution")
    @Override
    public Job getWorkflowExecution(@PathVariable String workflowId) {
        return workflowJobService.getJob(Long.valueOf(workflowId));
    }

    @RequestMapping(value = "/jobs/{tenantPid}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of workflow executions for a tenant")
    @Override
    public List<Job> getWorkflowExecutionsForTenant(@PathVariable Long tenantPid) {
        return workflowJobService.getJobsByTenantPid(tenantPid);
    }

    @RequestMapping(value = "/jobs", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of workflow executions given list of job Ids")
    @Override
    public List<Job> getWorkflowExecutionsByJobIds(@RequestParam(value = "jobId") List<String> jobIds) {
//        List<WorkflowExecutionId> workflowExecutionIds = jobIds.stream()
//                .map(jobId -> new WorkflowExecutionId((Long.valueOf(jobId)))).collect(Collectors.toList());
//        return workflowService.getJobs(workflowExecutionIds);
        List<Long> workflowIds = jobIds.stream().map(Long::valueOf).collect(Collectors.toList());
        return workflowJobService.getJobs(workflowIds);
    }

    @RequestMapping(value = "/jobs/{tenantPid}/find", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of workflow executions for a tenant filtered by job type")
    @Override
    public List<Job> getWorkflowExecutionsForTenant(@PathVariable long tenantPid,
                                                    @RequestParam(value = "type") String type) {
//        List<Job> jobs = workflowContainerService.getJobsByTenant(tenantPid, type);
//        return jobs;
        return workflowJobService.getJobsByTenantPid(tenantPid, Collections.singletonList(type), true);
    }
}