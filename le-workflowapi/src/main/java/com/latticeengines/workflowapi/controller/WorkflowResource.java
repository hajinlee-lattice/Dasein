package com.latticeengines.workflowapi.controller;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.base.Preconditions;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.FakeApplicationId;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.JobRequest;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflowapi.WorkflowLogLinks;
import com.latticeengines.workflow.exposed.service.WorkflowReportService;
import com.latticeengines.workflowapi.service.WorkflowContainerService;
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

    private static final String AUTO_RETRY_USER = "Auto Retry";

    @Inject
    private JobEntityMgr jobEntityMgr;

    @Inject
    private WorkflowJobService workflowJobService;

    @Inject
    private WorkflowContainerService workflowContainerService;

    @Value("${workflow.jobs.pa.hideRetried}")
    private boolean hideRetriedPA;

    @Inject
    private WorkflowReportService workflowReportService;

    @PostMapping("/job/{workflowId}/stop")
    @ApiOperation(value = "Stop an executing workflow")
    public void stopWorkflowExecution(@PathVariable String workflowId,
            @RequestParam(required = false) String customerSpace) {
        workflowJobService.stopWorkflow(customerSpace, Long.valueOf(workflowId));
    }

    @PostMapping("/job/{workflowId}/restart")
    @ApiOperation(value = "Restart a previous workflow execution")
    public AppSubmission restartWorkflowExecution(@PathVariable String workflowId, @RequestParam String customerSpace,
            @ApiParam(value = "Memory in MB", required = false) @RequestParam(value = "memory", required = false) String memoryStr,
            @RequestParam(value = "autoRetry", required = false, defaultValue = "false") Boolean autoRetry) {
        long wfId = Long.valueOf(workflowId);

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
        // set restart workflow job id
        if (job.getInputs() == null) {
            job.setInputs(new HashMap<>());
        }
        job.getInputs().put(WorkflowContextConstants.Inputs.RESTART_JOB_ID, String.valueOf(wfId));

        Map<String, String> tags = new HashMap<>(MapUtils.emptyIfNull(job.getTags()));
        // only set if this is first retry
        tags.putIfAbsent(WorkflowContextConstants.Tags.ROOT_WORKFLOW_PID, job.getPid().toString());
        tags.putIfAbsent(WorkflowContextConstants.Tags.ROOT_WORKFLOW_START_TIME,
                String.valueOf(job.getStartTimestamp().getTime()));
        // parent link
        tags.putIfAbsent(WorkflowContextConstants.Tags.PARENT_WORKFLOW_PID,
                job.getPid().toString());
        job.setTags(tags);

        workflowConfig.setInputProperties(job.getInputs());
        if (Boolean.TRUE.equals(autoRetry)) {
            workflowConfig.setUserId(AUTO_RETRY_USER);
        } else {
            workflowConfig.setUserId(job.getUser());
        }
        int memory = StringUtils.isNotBlank(memoryStr) ? Integer.parseInt(memoryStr) : 0;
        setupMemory(memory, job, workflowConfig);

        AppSubmission submission = new AppSubmission(
                workflowJobService.submitWorkflow(customerSpace, workflowConfig, null));
        if (hideRetriedPA && ProcessAnalyzeWorkflowConfiguration.WORKFLOW_NAME.equalsIgnoreCase(job.getName())) {
            // update status of retried job
            log.info("Updating retried PA job status, workflowId = {}", wfId);
            workflowJobService.updateWorkflowStatusAfterRetry(customerSpace, wfId);
        }
        return submission;
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

    @GetMapping("/job/{workflowId}")
    @ApiOperation(value = "Get a workflow execution")
    public Job getWorkflowExecution(@PathVariable String workflowId,
            @RequestParam(required = false) String customerSpace,
            @RequestParam(required = false, defaultValue = "false") Boolean bypassCache) {
        if (bypassCache) {
            return workflowJobService.getJobByWorkflowId(customerSpace, Long.valueOf(workflowId), true);
        } else {
            return workflowJobService.getJobByWorkflowIdFromCache(customerSpace, Long.valueOf(workflowId), true);
        }
    }

    @GetMapping("/job/{workflowPid}/setErrorCategory")
    @ApiOperation(value = "set error_category")
    public void setErrorCategoryByJobId(@PathVariable String workflowPid, @RequestParam String customerSpace,
            @RequestParam String errorCategory) {
        try {
            workflowJobService.setErrorCategoryByJobPid(customerSpace, Long.valueOf(workflowPid),
                    URLDecoder.decode(errorCategory, "utf-8"));
        } catch (UnsupportedEncodingException e) {
            log.error(e.getMessage());
        }
    }

    @GetMapping("/jobs")
    @ApiOperation(value = "Get list of workflow jobs by given list of job Ids or job types.")
    public List<Job> getJobs(@RequestParam(value = "jobId", required = false) List<String> jobIds,
                             @RequestParam(value = "type", required = false) List<String> types,
                             @RequestParam(value = "status", required = false) List<String> statuses,
                             @RequestParam(value = "includeDetails", required = false) Boolean includeDetails,
                             @RequestParam(value = "limitMaxRow", required = false) Boolean limitMaxRow,
                             @RequestParam(required = false) String customerSpace) {
        Optional<List<String>> optionalJobIds = Optional.ofNullable(jobIds);
        Optional<List<String>> optionalTypes = Optional.ofNullable(types);
        Optional<List<String>> optionalStatuses = Optional.ofNullable(statuses);
        Optional<Boolean> optionalIncludeDetails = Optional.ofNullable(includeDetails);
        Optional<Boolean> optionalLimitMaxRow = Optional.ofNullable(limitMaxRow);
        List<Long> workflowIds = null;
        if (optionalJobIds.isPresent() && !jobIds.isEmpty()) {
            workflowIds = optionalJobIds.get().stream().map(Long::valueOf).collect(Collectors.toList());
        }
        if (CollectionUtils.isNotEmpty(workflowIds) && !optionalTypes.isPresent() && !optionalStatuses.isPresent()) {
            // from cache
            return workflowJobService.getJobsByWorkflowIdsFromCache(customerSpace, workflowIds, optionalIncludeDetails.orElse(true));
        } else if (optionalTypes.isPresent() || optionalStatuses.isPresent()) {
            return workflowJobService.getJobsByWorkflowIds(customerSpace, workflowIds, optionalTypes.orElse(null),
                    optionalStatuses.orElse(null), optionalIncludeDetails.orElse(true), false, -1L);
        } else {
            return workflowJobService.getJobsByCustomerSpaceFromCache(customerSpace,
                    optionalIncludeDetails.orElse(true), optionalLimitMaxRow.orElse(false));
        }
    }

    @GetMapping("/clusters/current/jobs/count")
    @ApiOperation(value = "Get the number of workflow jobs that are not in terminal state in current cluster.")
    public Integer getNonTerminalJobCount( //
            @RequestParam(required = false) String customerSpace, //
            @RequestParam(value = "type", required = false) List<String> types) {
        return workflowJobService.getNonTerminalJobCount(customerSpace, types);
    }

    @PostMapping("/jobsByPid")
    @ApiOperation(value = "Get list of workflow jobs by given list of workflowPid or job types.")
    public List<Job> getJobsByPid(@RequestBody JobRequest request) {
        Optional<List<String>> optionalJobIds = Optional.ofNullable(request.getJobIds());
        Optional<List<String>> optionalTypes = Optional.ofNullable(request.getTypes());
        Optional<Boolean> optionalIncludeDetails = Optional.ofNullable(request.getIncludeDetails());
        if (optionalJobIds.isPresent()) {
            List<Long> workflowIds = optionalJobIds.get().stream().map(Long::valueOf).collect(Collectors.toList());
            return workflowJobService.getJobsByWorkflowPids(request.getCustomerSpace(), workflowIds,
                    optionalTypes.orElse(null), optionalIncludeDetails.orElse(true), false, -1L);
        } else if (optionalTypes.isPresent()) {
            return workflowJobService.getJobsByWorkflowPids(request.getCustomerSpace(), null, optionalTypes.get(),
                    optionalIncludeDetails.orElse(true), false, -1L);
        } else {
            return workflowJobService.getJobsByCustomerSpaceFromCache(request.getCustomerSpace(),
                    optionalIncludeDetails.orElse(true), false);
        }
    }

    @PutMapping("/jobs")
    @ApiOperation(value = "Update workflow jobs' parent job Id")
    public void updateParentJobId(@RequestParam(value = "jobId", required = true) List<String> jobIds,
            @RequestParam(value = "parentId", required = true) String parentJobId,
            @RequestParam(required = false) String customerSpace) {
        workflowJobService.updateParentJobIdByWorkflowIds(customerSpace,
                jobIds.stream().map(Long::valueOf).collect(Collectors.toList()), Long.valueOf(parentJobId));
    }

    @PostMapping("/jobs/submit")
    @ApiOperation(value = "Create a workflow execution in a Yarn container")
    public AppSubmission submitWorkflowExecution(@RequestBody WorkflowConfiguration config,
            @RequestParam(required = false) String customerSpace) {
        return new AppSubmission(workflowJobService.submitWorkflow(customerSpace, config, null));
    }

    @PostMapping("/jobs/submitwithpid")
    @ApiOperation(value = "Create a workflow execution in a Yarn container")
    public AppSubmission submitWorkflow(@RequestBody WorkflowConfiguration config,
            @RequestParam(required = true) Long workflowPid, @RequestParam(required = false) String customerSpace) {
        return new AppSubmission(workflowJobService.submitWorkflow(customerSpace, config, workflowPid));
    }

    @PostMapping("/awsJobs/submit")
    @ApiOperation(value = "Create a workflow execution in a AWS container")
    public String submitAWSWorkflowExecution(@RequestBody WorkflowConfiguration workflowConfig,
            @RequestParam(required = false) String customerSpace) {
        return workflowJobService.submitAwsWorkflow(customerSpace, workflowConfig);
    }

    @PostMapping("/jobs/create")
    @ApiOperation(value = "Create a workflow job")
    public Long createWorkflowJob(@RequestParam(required = true) String customerSpace) {
        return workflowJobService.createWorkflowJob(customerSpace);
    }

    @PostMapping("/jobs/createfail")
    @ApiOperation(value = "Create a failed workflow job record")
    public Long createFailedWorkflowJob(@RequestParam String customerSpace, @RequestBody Job failedJob) {
        return workflowJobService.createFailedWorkflowJob(customerSpace, failedJob);
    }

    @GetMapping("/yarnapps/id/{applicationId}")
    @ApiOperation(value = "Get workflowId from the applicationId of a workflow execution in a Yarn container")
    public WorkflowExecutionId getWorkflowId(@PathVariable String applicationId,
            @RequestParam(required = false) String customerSpace) {
        if (ApplicationIdUtils.isFakeApplicationId(applicationId)) {
            return workflowJobService.getWorkflowExecutionIdByWorkflowPid(customerSpace,
                    FakeApplicationId.toWorkflowJobPid(applicationId));
        }
        return workflowJobService.getWorkflowExecutionIdByApplicationId(customerSpace, applicationId);
    }

    @GetMapping("/yarnapps/job/{applicationId}")
    @ApiOperation(value = "Get status about a submitted workflow from YARN applicationId")
    public Job getWorkflowJobFromApplicationId(@PathVariable String applicationId,
            @RequestParam(required = false) String customerSpace) {
        if (ApplicationIdUtils.isFakeApplicationId(applicationId)) {
            return workflowJobService.getJobByWorkflowPid(customerSpace,
                    FakeApplicationId.toWorkflowJobPid(applicationId), true);
        }
        return workflowJobService.getJobByApplicationId(customerSpace, applicationId, true);
    }

    @DeleteMapping("/yarnapps/job/{applicationId}")
    @ApiOperation(value = "Delete a workflow from YARN applicationId")
    public WorkflowJob deleteWorkflowJobFromApplicationId(@PathVariable String applicationId,
            @RequestParam(required = false) String customerSpace) {
        return workflowJobService.deleteWorkflowJobByApplicationId(customerSpace, applicationId);
    }

    @DeleteMapping("/yarnapps/job/deletebytenant/{tenantPid}")
    @ApiOperation(value = "Delete a workflow from YARN applicationId")
    public void deleteByTenantPid(@PathVariable Long tenantPid, @RequestParam(required = false) String customerSpace) {
        workflowJobService.deleteByTenantPid(customerSpace, tenantPid);
    }

//    @DeleteMapping("/yarnapps/job/customerSpace/{customerSpace}/cancelWorkflow/{workflowJobPid}")
//    @ApiOperation(value = "cancel a workflow")
//    public ResponseDocument<Boolean> cancelByWorkflowJobPid(@PathVariable String customerSpace, @PathVariable Long workflowJobPid) {
//        return ResponseDocument.successResponse(workflowJobService.cancelWorkflowByWorkflowJobPid(customerSpace, workflowJobPid));
//    }

    @DeleteMapping("/yarnapps/job/customerSpace/{customerSpace}/cancelWorkflow/applications/{applicationId}")
    @ApiOperation(value = "cancel a workflow")
    public ResponseDocument<Boolean> cancelByApplicationId(@PathVariable String customerSpace, @PathVariable String applicationId) {
        return ResponseDocument.successResponse(workflowJobService.cancelWorkflowByApplicationId(customerSpace, applicationId));
    }

    @DeleteMapping("/caches/jobs")
    @ApiOperation(value = "Delete all job cache entries")
    public int clearJobCaches() {
        return workflowJobService.clearAllJobCaches();
    }

    @DeleteMapping("/caches/jobs/{customerSpace}")
    @ApiOperation(value = "Delete all job cache entries for specified tenant")
    public int clearJobCachesForTenant(@PathVariable String customerSpace,
            @RequestParam(required = false) List<Long> workflowIds) {
        Preconditions.checkArgument(StringUtils.isNotBlank(customerSpace), "Should specify a valid customerSpace");
        if (workflowIds == null) {
            log.info("Clearing all job cache entries for customerSpace = {}", customerSpace);
            return workflowJobService.clearJobCaches(customerSpace);
        } else {
            log.info("Clearing job cache entries for customerSpace = {}, workflowIds = {}", customerSpace, workflowIds);
            return workflowJobService.clearJobCachesByWorkflowIds(customerSpace, workflowIds);
        }
    }

    @DeleteMapping("/yarnapps/jobs")
    @ApiOperation(value = "Delete a workflow by tenant, job type and timestamps range")
    public List<WorkflowJob> deleteWorkflowJobs(@RequestParam String customerSpace, @RequestParam String type,
            @RequestParam Long startTime, @RequestParam Long endTime) {
        return workflowJobService.deleteWorkflowJobs(customerSpace, type, startTime, endTime);
    }

    @GetMapping("/log-link/pid/{workflowPid}")
    @ApiOperation("Get log url for a workflow by pid.")
    public WorkflowLogLinks getLogLinkByWorkflowPid(@PathVariable long workflowPid) {
        return workflowContainerService.getLogUrlByWorkflowPid(workflowPid);
    }

    @GetMapping("/jobsbycluster")
    @ApiOperation(value = "Get list of workflow jobs by given clusterId or list of job types or job statuses.")
    public List<WorkflowJob> jobsByCluster(@RequestParam(required = false) String clusterId,
            @RequestParam(value = "tenantPid", required = false) Long tenantPid,
            @RequestParam(value = "type", required = false) List<String> workflowTypes,
            @RequestParam(value = "status", required = false) List<String> statuses,
            @RequestParam(value = "earliestStartTime", required = false) Long earliestStartTime) {
        return workflowJobService.queryByClusterIDAndTypesAndStatuses(clusterId, tenantPid, workflowTypes, statuses,
                earliestStartTime);
    }

    @GetMapping("/jobs/{customerSpace}/{workflowPid}")
    @ApiOperation("Get workflowJob object by PID")
    public Job getJobByWorkflowJobPid(@PathVariable String customerSpace, @PathVariable Long workflowPid,
            @RequestParam(value = "includeDetails", required = false) boolean includeDetails) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        Job job = workflowJobService.getJobByWorkflowPid(customerSpace, workflowPid, includeDetails);
        if (job == null) {
            throw new LedpException(LedpCode.LEDP_28000, new String[] { workflowPid.toString() });
        }
        return job;
    }

    @GetMapping("/workflowJobs/{customerSpace}/{workflowPid}")
    @ApiOperation("Get workflowJob object by PID")
    public WorkflowJob getWorkflowJobByWorkflowJobPid(@PathVariable String customerSpace,
            @PathVariable Long workflowPid) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        WorkflowJob workflowJob = workflowJobService.getWorkflowJobByPid(customerSpace, workflowPid);
        if (workflowJob == null) {
            throw new LedpException(LedpCode.LEDP_28000, new String[] { workflowPid.toString() });
        }
        return workflowJob;
    }

    @GetMapping("/workflowJobs/{customerSpace}/{workflowPid}/jobStatus")
    @ApiOperation("Get workflow JobStatus by PID")
    public String getJobStatusByWorkflowJobPid(@PathVariable String customerSpace, @PathVariable Long workflowPid) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        WorkflowJob workflowJob = workflowJobService.getWorkflowJobByPid(customerSpace, workflowPid);
        if (workflowJob == null) {
            throw new LedpException(LedpCode.LEDP_28000, new String[] { workflowPid.toString() });
        }
        return workflowJob.getStatus();
    }

    @GetMapping("/workflowJobs/{customerSpace}/{workflowPid}/applicationId")
    @ApiOperation("Get applicationId by PID")
    public String getApplicationIdByWorkflowJobPid(@PathVariable String customerSpace, @PathVariable Long workflowPid) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        WorkflowJob workflowJob = workflowJobService.getWorkflowJobByPid(customerSpace, workflowPid);
        if (workflowJob == null) {
            throw new LedpException(LedpCode.LEDP_28000, new String[] { workflowPid.toString() });
        }
        return workflowJob.getApplicationId();
    }

    @GetMapping("/throttling/flag")
    public boolean getThrottlingStackFlag() {
        String podid = CamilleEnvironment.getPodId();
        String division = CamilleEnvironment.getDivision();
        Camille c = CamilleEnvironment.getCamille();
        try {
            return Boolean.parseBoolean(c.get(PathBuilder.buildWorkflowThrottlingFlagPath(podid, division)).getData());
        } catch (Exception e) {
            log.error("Unable to read flag value from zk {}-{}. The flag value is considered false.", podid, division);
            return false;
        }
    }

    @PostMapping("/throttling/flag")
    public boolean setThrottlingStackFlag(@RequestBody boolean flag) {
        String podid = CamilleEnvironment.getPodId();
        String division = CamilleEnvironment.getDivision();
        Camille c = CamilleEnvironment.getCamille();
        Path flagPath = PathBuilder.buildWorkflowThrottlingFlagPath(podid, division);
        try {
            if (c.exists(flagPath)) {
                c.set(flagPath, new Document(Boolean.toString(flag)));
            } else {
                c.create(flagPath, new Document(Boolean.toString(flag)), ZooDefs.Ids.OPEN_ACL_UNSAFE);
            }
            return true;
        } catch (Exception e) {
            log.error("Unable to set flag value for {} - {}.", podid, division);
            return false;
        }
    }

    @PostMapping("/report")
    @ApiOperation(value = "create or update a report")
    public void createOrUpdateReport(@RequestParam(value = "customerSpace") String customerSpace, @RequestBody Report report) {
        workflowReportService.createOrUpdateReport(customerSpace, report);
    }

    @DeleteMapping("/report/{name}")
    @ApiOperation(value = "delete a report")
    public void deleteReportByName(@PathVariable String name,
                                   @RequestParam(value = "customerSpace") String customerSpace) {
        workflowReportService.deleteReportByName(customerSpace, name);
    }

    @GetMapping("/report/{name}")
    @ApiOperation(value = "Get a report by name")
    public Report findReportByName(@PathVariable String name,
                                   @RequestParam(value = "customerSpace") String customerSpace) {
        return workflowReportService.findReportByName(customerSpace, name);
    }

    @GetMapping("/report")
    @ApiOperation(value = "Get all reports")
    public List<Report> findAll(@RequestParam(value = "customerSpace") String customerSpace) {
        return workflowReportService.findAll(customerSpace);
    }
}
