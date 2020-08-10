package com.latticeengines.apps.cdl.controller;

import static com.latticeengines.apps.cdl.service.SchedulingPAService.ACTIVE_STACK_SCHEDULER_NAME;
import static com.latticeengines.apps.cdl.service.SchedulingPAService.INACTIVE_STACK_SCHEDULER_NAME;
import static com.latticeengines.domain.exposed.cdl.scheduling.SchedulerConstants.PA_JOB_TYPE;
import static com.latticeengines.domain.exposed.cdl.scheduling.SchedulerConstants.RECENT_PA_LOOK_BACK_DAYS;
import static java.util.Collections.singletonList;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.ActiveStackInfoService;
import com.latticeengines.apps.cdl.service.CDLJobService;
import com.latticeengines.apps.cdl.service.PAQuotaService;
import com.latticeengines.apps.cdl.service.SchedulingPAService;
import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.scheduling.PAQuotaSummary;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingStatus;
import com.latticeengines.domain.exposed.monitor.annotation.NoMetricsLog;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.monitor.exposed.annotation.IgnoreGlobalApiMeter;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "paScheduler")
@RestController
@RequestMapping("/schedulingPAQueue")
public class SchedulingPAQueueResource {

    @Inject
    private SchedulingPAService schedulingPAService;

    @Inject
    private ActiveStackInfoService activeStackInfoService;

    @Inject
    private CDLJobService cdlJobService;

    @Inject
    private PAQuotaService paQuotaService;

    @Inject
    private WorkflowProxy workflowProxy;

    @GetMapping("/getQueueInfo")
    @ResponseBody
    @ApiOperation(value = "getQueueInfo")
    @NoMetricsLog
    @IgnoreGlobalApiMeter
    @NoCustomerSpace
    public Map<String, List<String>> getQueueInfo(@RequestParam String schedulerName) {
        return schedulingPAService.showQueue(schedulerName);
    }

    @GetMapping("/getPosition")
    @ResponseBody
    @ApiOperation(value = "getPosition")
    @NoMetricsLog
    @IgnoreGlobalApiMeter
    @NoCustomerSpace
    public String getPosition(@RequestParam String tenantName, @RequestParam String schedulerName) {
        return schedulingPAService.getPositionFromQueue(schedulerName, tenantName);
    }

    @PutMapping("/triggerSchedulingPA/{schedulerName}")
    @ResponseBody
    @ApiOperation(value = "Trigger Scheduling PA for given scheduler")
    @NoMetricsLog
    @IgnoreGlobalApiMeter
    @NoCustomerSpace
    public void triggerSchedulingPA(@PathVariable String schedulerName,
            @RequestParam(required = false) boolean dryRun) {
        cdlJobService.schedulePAJob(schedulerName, dryRun);
    }

    @GetMapping("/isActivityBasedPA/{schedulerName}")
    @ResponseBody
    @ApiOperation(value = "get ActivityBasedPA Flag for a specific scheduler")
    @NoMetricsLog
    @IgnoreGlobalApiMeter
    @NoCustomerSpace
    public Boolean isActivityBasedPA(@PathVariable String schedulerName) {
        return schedulingPAService.isSchedulerEnabled(schedulerName);
    }

    @GetMapping("/isActivityBasedPA")
    @ResponseBody
    @ApiOperation(value = "get ActivityBasedPA Flag for current stack")
    @NoCustomerSpace
    public Boolean isActivityBasedPA() {
        boolean isActive = activeStackInfoService.isCurrentStackActive();
        return schedulingPAService
                .isSchedulerEnabled(isActive ? ACTIVE_STACK_SCHEDULER_NAME : INACTIVE_STACK_SCHEDULER_NAME);
    }

    @GetMapping("/status/{customerSpace}")
    @ResponseBody
    @ApiOperation("Retrieve all scheduler-related information for a specific tenant")
    public SchedulingStatus getSchedulingStatus(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        boolean isActive = activeStackInfoService.isCurrentStackActive();
        return schedulingPAService.getSchedulingStatus(customerSpace,
                isActive ? ACTIVE_STACK_SCHEDULER_NAME : INACTIVE_STACK_SCHEDULER_NAME);
    }

    @GetMapping("/quota/{customerSpace}")
    @ResponseBody
    @ApiOperation("Retrieve PA quota for specific tenant")
    public PAQuotaSummary getPAQuota(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        String shortTenantId = CustomerSpace.shortenCustomerSpace(customerSpace);

        // TODO only query jobs from specific tenant
        // TODO get tenant specific timezone
        long earliestStartTime = Instant.now().minus(RECENT_PA_LOOK_BACK_DAYS, ChronoUnit.DAYS).toEpochMilli();
        List<WorkflowJob> completedPAJobs = workflowProxy.queryByClusterIDAndTypesAndStatuses(null,
                singletonList(PA_JOB_TYPE), singletonList(JobStatus.COMPLETED.getName()), earliestStartTime);
        List<WorkflowJob> completedJobs = CollectionUtils.emptyIfNull(completedPAJobs).stream() //
                .filter(Objects::nonNull) //
                .filter(job -> job.getTenant() != null && StringUtils.isNotBlank(job.getTenant().getId())) //
                .filter(job -> {
                    String tenantId = CustomerSpace.shortenCustomerSpace(job.getTenant().getId());
                    return shortTenantId.equals(tenantId);
                }) //
                .collect(Collectors.toList());
        return paQuotaService.getPAQuotaSummary(customerSpace, completedJobs, null, null);
    }
}
