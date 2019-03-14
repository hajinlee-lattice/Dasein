package com.latticeengines.pls.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.pls.service.WorkflowJobService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "jobs", description = "REST resource for jobs")
@RestController
@RequestMapping("/jobs")
@PreAuthorize("hasRole('View_PLS_Jobs')")
public class JobResource {

    @Inject
    private WorkflowJobService workflowJobService;

    @GetMapping(value = "/{jobId}")
    @ResponseBody
    @ApiOperation(value = "Get a job by id")
    public Job find(@PathVariable String jobId, //
            @RequestParam(value = "type", required = false) String type) {
        return workflowJobService.find(jobId, true);
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Retrieve all jobs")
    public List<Job> findAll( //
            @RequestParam(value = "jobId", required = false) List<String> jobIds, //
            @RequestParam(value = "type", required = false) List<String> types, //
            @RequestParam(value = "status", required = false) List<String> jobStatuses, //
            @RequestParam(value = "includeDetails", required = false) Boolean includeDetails, //
            @RequestParam(value = "hasParentId", required = false) Boolean hasParentId, //
            @RequestParam(value = "filterNonUiJobs", required = false, defaultValue = "true") Boolean filterNonUiJobs, //
            @RequestParam(value = "includeEmptyPA", required = false, defaultValue = "true") Boolean includeEmptyPA //
    ) {
        if (jobIds == null && types == null && includeDetails == null && hasParentId == null && jobStatuses == null) {
            return workflowJobService.findAll(filterNonUiJobs, includeEmptyPA);
        }
        // TODO ygao this if statement will be removed when le-workflow work
        // service layer is completed
        if (jobIds != null && types == null && includeDetails == null && hasParentId == null && jobStatuses == null) {
            return workflowJobService.findByJobIds(jobIds, filterNonUiJobs, includeEmptyPA);
        }
        return workflowJobService.findJobs(jobIds, types, jobStatuses, includeDetails, false, filterNonUiJobs,
                includeEmptyPA);
    }

    @RequestMapping(value = "/yarnapps/{applicationId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Retrieve job from yarn application id")
    public Job findByApplicationId(@PathVariable String applicationId) {
        return workflowJobService.findByApplicationId(applicationId);
    }

    @RequestMapping(value = "/find", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Find jobs with the provided job type")
    public List<Job> findAllWithType(@RequestParam("type") String type) {
        return workflowJobService.findAllWithType(type);
    }

    @RequestMapping(value = "/{jobId}/cancel", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Cancel a running job")
    @PreAuthorize("hasRole('Edit_PLS_Jobs')")
    public void cancel(@PathVariable String jobId) {
        workflowJobService.cancel(jobId);
    }

    @RequestMapping(value = "/{jobPid}/setErrorCategory", method = RequestMethod.GET, headers = "Accept" +
            "=application/json")
    @ResponseBody
    @ApiOperation(value = "set error_category")
    @PreAuthorize("hasRole('Edit_PLS_Jobs')")
    public void setErrorCategoryByJobId(@PathVariable String jobPid, @RequestParam(value="errorCategory",
            required = false) String errorCategory) {
        workflowJobService.setErrorCategoryByJobPid(jobPid, errorCategory);
    }

    @RequestMapping(value = "/{jobId}/restart", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Restart a previous job")
    @PreAuthorize("hasRole('Edit_PLS_Jobs')")
    public ResponseDocument<String> restart(@PathVariable Long jobId) {
        return ResponseDocument.successResponse(workflowJobService.restart(jobId).toString());
    }

    @RequestMapping(value = "/{jobId}/report/download", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Generate P&A job report")
    public ResponseDocument<String> downloadReport(@PathVariable String jobId) {
        return ResponseDocument.successResponse(workflowJobService.generateCSVReport(jobId));
    }
}
