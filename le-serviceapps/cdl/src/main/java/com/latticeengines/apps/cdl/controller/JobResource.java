package com.latticeengines.apps.cdl.controller;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.DLTenantMappingService;
import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataloader.DLTenantMapping;
import com.latticeengines.domain.exposed.dataloader.JobStatusResult;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "jobs", description = "REST resource for CDL jobs")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/jobs")
public class JobResource {

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private DLTenantMappingService dlTenantMappingService;

    @RequestMapping(value = "/{applicationId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get status about a submitted job")
    @NoCustomerSpace
    public JobStatusResult getJobStatus(@PathVariable String customerSpace, @PathVariable String applicationId) {
        CustomerSpace cs = CustomerSpace.parse(customerSpace);
        DLTenantMapping tenantMapping = dlTenantMappingService.getDLTenantMapping(cs.getTenantId(), "*");
        customerSpace = tenantMapping == null ? cs.toString() : CustomerSpace.parse(tenantMapping.getTenantId()).toString();
        Job job = workflowProxy.getWorkflowJobFromApplicationId(applicationId, customerSpace);
        if (job != null) {
            JobStatusResult jobStatusResult = new JobStatusResult();
            JobStatus jobStatus = job.getJobStatus();
            if (jobStatus == JobStatus.COMPLETED) {
                jobStatusResult.setStatus(FinalApplicationStatus.SUCCEEDED);
            } else if (jobStatus == JobStatus.FAILED) {
                jobStatusResult.setStatus(FinalApplicationStatus.FAILED);
            } else if (jobStatus == JobStatus.CANCELLED || jobStatus == JobStatus.SKIPPED) {
                jobStatusResult.setStatus(FinalApplicationStatus.KILLED);
            } else {
                jobStatusResult.setStatus(FinalApplicationStatus.UNDEFINED);
            }
            jobStatusResult.setDiagnostics(job.getErrorMsg());

            return jobStatusResult;
        } else {
            throw new RuntimeException(String.format("Cannot get workflow job by applicationId %s.", applicationId));
        }
    }
}
