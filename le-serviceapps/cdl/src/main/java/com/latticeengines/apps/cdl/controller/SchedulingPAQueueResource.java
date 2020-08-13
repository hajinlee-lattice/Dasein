package com.latticeengines.apps.cdl.controller;

import static com.latticeengines.apps.cdl.service.SchedulingPAService.ACTIVE_STACK_SCHEDULER_NAME;
import static com.latticeengines.apps.cdl.service.SchedulingPAService.INACTIVE_STACK_SCHEDULER_NAME;
import static com.latticeengines.domain.exposed.cdl.scheduling.SchedulerConstants.PA_JOB_TYPE;
import static com.latticeengines.domain.exposed.cdl.scheduling.SchedulerConstants.RECENT_PA_LOOK_BACK_DAYS;
import static java.util.Collections.singletonList;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.cdl.service.ActiveStackInfoService;
import com.latticeengines.apps.cdl.service.CDLJobService;
import com.latticeengines.apps.cdl.service.PAQuotaService;
import com.latticeengines.apps.cdl.service.SchedulingPAService;
import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.scheduling.PAQuotaSummary;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingStatus;
import com.latticeengines.domain.exposed.monitor.annotation.NoMetricsLog;
import com.latticeengines.domain.exposed.security.Tenant;
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

    private static final Logger log = LoggerFactory.getLogger(SchedulingPAQueueResource.class);

    @Inject
    private SchedulingPAService schedulingPAService;

    @Inject
    private ActiveStackInfoService activeStackInfoService;

    @Inject
    private BatonService batonService;

    @Inject
    private CDLJobService cdlJobService;

    @Inject
    private PAQuotaService paQuotaService;

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

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
        SchedulingStatus status = schedulingPAService.getSchedulingStatus(customerSpace,
                isActive ? ACTIVE_STACK_SCHEDULER_NAME : INACTIVE_STACK_SCHEDULER_NAME);
        if (status.isSchedulerEnabled()) {
            try {
                PAQuotaSummary summary = getPAQuota(customerSpace);
                if (summary != null) {
                    status.setRemainingPaQuota(summary.getRemainingPaQuota());
                    status.setHandHoldPATenant(summary.isHandHoldPATenant());
                }
            } catch (Exception e) {
                log.error("Failed to get PA quota summary for tenant {}, error = {}", customerSpace, e);
            }
        }
        return status;
    }

    @GetMapping("/quota/{customerSpace}")
    @ResponseBody
    @ApiOperation("Retrieve PA quota for specific tenant")
    public PAQuotaSummary getPAQuota(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        String shortTenantId = CustomerSpace.shortenCustomerSpace(customerSpace);

        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace);
        Preconditions.checkArgument(tenant != null && tenant.getPid() != null,
                String.format("cannot find tenant with ID [%s]", customerSpace));
        Pair<ZoneId, Boolean> result = getTenantTimezone(customerSpace);
        ZoneId timezone = result.getKey();
        long earliestStartTime = Instant.now().minus(RECENT_PA_LOOK_BACK_DAYS, ChronoUnit.DAYS).toEpochMilli();
        List<WorkflowJob> completedPAJobs = workflowProxy.queryByClusterIDAndTypesAndStatuses(null,
                tenant.getPid(), singletonList(PA_JOB_TYPE), singletonList(JobStatus.COMPLETED.getName()),
                earliestStartTime);
        log.info("Retrieving PA quota info for tenant {} (timezone = {}), no. completed PAs in last {} days = {}",
                customerSpace, timezone, RECENT_PA_LOOK_BACK_DAYS, CollectionUtils.size(completedPAJobs));
        List<WorkflowJob> completedJobs = CollectionUtils.emptyIfNull(completedPAJobs).stream() //
                .filter(Objects::nonNull) //
                .filter(job -> job.getTenant() != null && StringUtils.isNotBlank(job.getTenant().getId())) //
                .filter(job -> {
                    String tenantId = CustomerSpace.shortenCustomerSpace(job.getTenant().getId());
                    return shortTenantId.equals(tenantId);
                }) //
                .collect(Collectors.toList());
        PAQuotaSummary summary = paQuotaService.getPAQuotaSummary(customerSpace, completedJobs, null, timezone);
        if (timezone != null) {
            summary.setTimezone(timezone.toString());
        } else if (result.getRight()) {
            // TODO return invalid timezone value
            summary.setTimezone("Timezone configured for this tenant does not have the correct format");
        }
        return summary;
    }

    // timezone, flag to indicate parsing error
    private Pair<ZoneId, Boolean> getTenantTimezone(String customerSpace) {
        try {
            ZoneId timezone = batonService.getTenantTimezone(CustomerSpace.parse(customerSpace));
            return Pair.of(timezone, false);
        } catch (DateTimeException e) {
            log.error("timezone configured for tenant {} is not valid", customerSpace);
            return Pair.of(null, true);
        }
    }
}
