package com.latticeengines.workflowapi.controller;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.workflowapi.service.WorkflowJobService;
import com.latticeengines.yarn.exposed.client.ContainerProperty;
import com.latticeengines.yarn.exposed.entitymanager.JobEntityMgr;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "workflow", description = "REST resource for workflows")
@RestController
@RequestMapping("/workflows")
public class WorkflowResource {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(WorkflowResource.class);

    @Autowired
    private JobEntityMgr jobEntityMgr;

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
    public AppSubmission restartWorkflowExecution(@PathVariable String workflowId, @RequestParam String customerSpace,
            @ApiParam(value = "Memory in MB", required = false) @RequestParam(value = "memory", required = false) Integer memory) {
        Long wfId = Long.valueOf(workflowId);

        Job job = workflowJobService.getJobByWorkflowId(customerSpace, wfId, false);
        if (job == null) {
            throw new LedpException(LedpCode.LEDP_28017, new String[] { workflowId });
        } else if (job.getJobStatus() == null || !job.getJobStatus().isTerminated()) {
            throw new LedpException(LedpCode.LEDP_28018,
                    new String[] { workflowId, job.getJobStatus() == null ? null : job.getJobStatus().name() });
        }
        WorkflowConfiguration workflowConfig = new WorkflowConfiguration();
        workflowConfig.setWorkflowName(job.getName());
        workflowConfig.setRestart(true);
        workflowConfig.setWorkflowIdToRestart(new WorkflowExecutionId(wfId));
        workflowConfig.setCustomerSpace(CustomerSpace.parse(customerSpace));
        workflowConfig.setInputProperties(job.getInputs());
        workflowConfig.setUserId(job.getUser());
        setupMemory(memory, job, workflowConfig);

        return new AppSubmission(workflowJobService.submitWorkflow(customerSpace, workflowConfig, null));
    }

    private void setupMemory(Integer memory, Job job, WorkflowConfiguration workflowConfig) {
        if (memory != null && memory >= 1024 && memory <= 1024 * 50) {
            workflowConfig.setContainerMemoryMB(memory);
            log.info("Restart workflow with memory=" + memory + " job id=" + job.getApplicationId());
        } else {
            com.latticeengines.domain.exposed.dataplatform.Job yarnJob = jobEntityMgr
                    .findByObjectId(job.getApplicationId());
            if (yarnJob != null && yarnJob.getAppMasterPropertiesObject() != null) {
                String memStr = yarnJob.getAppMasterPropertiesObject().getProperty(ContainerProperty.MEMORY.toString());
                if (StringUtils.isNotEmpty(memStr))
                    workflowConfig.setContainerMemoryMB(Integer.parseInt(memStr));
                log.info("Restart workflow with existing memory=" + memStr + " job id=" + job.getApplicationId());
            }
        }
    }

    @RequestMapping(value = "/job/{workflowId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get a workflow execution")
    public Job getWorkflowExecution(@PathVariable String workflowId,
            @RequestParam(required = false) String customerSpace) {
        return workflowJobService.getJobByWorkflowId(customerSpace, Long.valueOf(workflowId), true);
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
            return workflowJobService.getJobsByWorkflowIds(customerSpace, workflowIds, optionalTypes.orElse(null),
                    optionalIncludeDetails.orElse(true), false, -1L);
        } else if (optionalTypes.isPresent()) {
            return workflowJobService.getJobsByWorkflowIds(customerSpace, null, optionalTypes.get(),
                    optionalIncludeDetails.orElse(true), false, -1L);
        } else {
            return workflowJobService.getJobsByCustomerSpace(customerSpace, optionalIncludeDetails.orElse(true));
        }
    }

    @RequestMapping(value = "/jobsByPid", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get list of workflow jobs by given list of workflowPid or job types.")
    public List<Job> getJobsByPid(@RequestParam(value = "jobId", required = false) List<String> jobIds,
                                  @RequestParam(value = "type", required = false) List<String> types,
                                  @RequestParam(value = "includeDetails", required = false) Boolean includeDetails,
                                  @RequestParam(required = false) String customerSpace) {
        Optional<List<String>> optionalJobIds = Optional.ofNullable(jobIds);
        Optional<List<String>> optionalTypes = Optional.ofNullable(types);
        Optional<Boolean> optionalIncludeDetails = Optional.ofNullable(includeDetails);

        if (optionalJobIds.isPresent()) {
            List<Long> workflowIds = optionalJobIds.get().stream().map(Long::valueOf).collect(Collectors.toList());
            return workflowJobService.getJobsByWorkflowPids(customerSpace, workflowIds, optionalTypes.orElse(null),
                    optionalIncludeDetails.orElse(true), false, -1L);
        } else if (optionalTypes.isPresent()) {
            return workflowJobService.getJobsByWorkflowPids(customerSpace, null, optionalTypes.get(),
                    optionalIncludeDetails.orElse(true), false, -1L);
        } else {
            return workflowJobService.getJobsByCustomerSpace(customerSpace, optionalIncludeDetails.orElse(true));
        }
    }

    @RequestMapping(value = "/jobs", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ApiOperation(value = "Update workflow jobs' parent job Id")
    public void updateParentJobId(@RequestParam(value = "jobId", required = true) List<String> jobIds,
            @RequestParam(value = "parentId", required = true) String parentJobId,
            @RequestParam(required = false) String customerSpace) {
        workflowJobService.updateParentJobIdByWorkflowIds(customerSpace,
                jobIds.stream().map(Long::valueOf).collect(Collectors.toList()), Long.valueOf(parentJobId));
    }

    @RequestMapping(value = "/jobs/submit", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Create a workflow execution in a Yarn container")
    public AppSubmission submitWorkflowExecution(@RequestBody WorkflowConfiguration config,
            @RequestParam(required = false) String customerSpace) {
        return new AppSubmission(workflowJobService.submitWorkflow(customerSpace, config, null));
    }

    @RequestMapping(value = "/jobs/submitwithpid", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Create a workflow execution in a Yarn container")
    public AppSubmission submitWorkflow(@RequestBody WorkflowConfiguration config,
            @RequestParam(required = true) Long workflowPid, @RequestParam(required = false) String customerSpace) {
        return new AppSubmission(workflowJobService.submitWorkflow(customerSpace, config, workflowPid));
    }

    @RequestMapping(value = "/awsJobs/submit", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Create a workflow execution in a AWS container")
    public String submitAWSWorkflowExecution(@RequestBody WorkflowConfiguration workflowConfig,
            @RequestParam(required = false) String customerSpace) {
        return workflowJobService.submitAwsWorkflow(customerSpace, workflowConfig);
    }

    @RequestMapping(value = "/jobs/create", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Create a workflow job")
    public Long createWorkflowJob(@RequestParam(required = true) String customerSpace) {
        return workflowJobService.createWorkflowJob(customerSpace);
    }

    @RequestMapping(value = "/yarnapps/id/{applicationId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get workflowId from the applicationId of a workflow execution in a Yarn container")
    public WorkflowExecutionId getWorkflowId(@PathVariable String applicationId,
            @RequestParam(required = false) String customerSpace) {
        return workflowJobService.getWorkflowExecutionIdByApplicationId(customerSpace, applicationId);
    }

    @RequestMapping(value = "/yarnapps/job/{applicationId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get status about a submitted workflow from a YARN application id")
    public Job getWorkflowJobFromApplicationId(@PathVariable String applicationId,
            @RequestParam(required = false) String customerSpace) {
        return workflowJobService.getJobByApplicationId(customerSpace, applicationId, true);
    }
}
