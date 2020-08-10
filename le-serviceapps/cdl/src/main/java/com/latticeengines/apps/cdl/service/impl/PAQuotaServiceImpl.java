package com.latticeengines.apps.cdl.service.impl;

import static com.latticeengines.domain.exposed.cdl.scheduling.SchedulerConstants.QUOTA_AUTO_SCHEDULE;
import static com.latticeengines.domain.exposed.cdl.scheduling.SchedulerConstants.QUOTA_SCHEDULE_NOW;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.cdl.service.PAQuotaService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.cdl.scheduling.PAQuotaSummary;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulerConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;

@Component("paQuotaService")
public class PAQuotaServiceImpl implements PAQuotaService {

    private static final Logger log = LoggerFactory.getLogger(PAQuotaServiceImpl.class);

    private static final long DEFAULT_SCHEDULE_NOW_QUOTA = 1;
    private static final long DEFAULT_AUTO_SCHEDULE_QUOTA = 1;
    private static final Map<String, Long> DEFAULT_QUOTA = ImmutableMap.of(QUOTA_SCHEDULE_NOW,
            DEFAULT_SCHEDULE_NOW_QUOTA, QUOTA_AUTO_SCHEDULE, DEFAULT_AUTO_SCHEDULE_QUOTA);

    @Override
    public boolean hasQuota(@NotNull String tenantId, @NotNull String quotaName, List<WorkflowJob> completedPAJobs,
            Instant now, ZoneId timezone) {
        Preconditions.checkArgument(StringUtils.isNotBlank(tenantId), "Tenant ID should not be blank");
        Preconditions.checkArgument(StringUtils.isNotBlank(quotaName), "PA quota name should not be blank");
        tenantId = CustomerSpace.shortenCustomerSpace(tenantId);

        Map<String, Long> quota = getTenantPaQuota(tenantId);
        if (!quota.containsKey(quotaName)) {
            log.debug("No {} quota configured for tenant {} at {}", quotaName, tenantId, now);
            return false;
        }
        if (now == null) {
            now = Instant.now();
        }
        if (timezone == null) {
            timezone = SchedulerConstants.DEFAULT_TIMEZONE;
        }

        Instant[] quotaInterval = getCurrentQuotaInterval(timezone, now);
        Instant quotaStartTime = quotaInterval[0];
        Instant quotaEndTime = quotaInterval[1];

        long allowed = quota.get(quotaName);
        long consumed = CollectionUtils.emptyIfNull(completedPAJobs) //
                .stream() //
                .filter(filterJobStartedInCurrentInterval(quotaStartTime, quotaEndTime)) //
                .filter(job -> {
                    // only count when consumed quota name match
                    // i.e., manually started PA (by PLO) will not count against any quota
                    String consumedQuotaName = getTagValue(job, WorkflowContextConstants.Tags.CONSUMED_QUOTA_NAME);
                    return quotaName.equals(consumedQuotaName);
                }) //
                .count();
        log.debug("No. consumed {} quota for tenant {} is {} (out of {}). now = {}, timezone = {}", quotaName, consumed,
                tenantId, allowed, now, timezone);
        return allowed >= consumed;
    }

    @Override
    public Map<String, Long> getTenantPaQuota(@NotNull String tenantId) {
        try {
            Camille c = CamilleEnvironment.getCamille();
            Path path = PathBuilder.buildTenantPaQuotaPath(CamilleEnvironment.getPodId(),
                    CustomerSpace.parse(tenantId));
            if (!c.exists(path)) {
                return DEFAULT_QUOTA;
            }

            String content = c.get(path).getData();
            Map<?, ?> rawMap = JsonUtils.deserialize(content, Map.class);
            Map<String, Long> quota = MapUtils.emptyIfNull(JsonUtils.convertMap(rawMap, String.class, Long.class));
            DEFAULT_QUOTA.forEach(quota::putIfAbsent);
            return quota;
        } catch (Exception e) {
            log.error("Failed to retrieve tenant level PA quota for tenant {}, error = {}", tenantId, e);
            return DEFAULT_QUOTA;
        }
    }

    @Override
    public PAQuotaSummary getPAQuotaSummary(@NotNull String tenantId, List<WorkflowJob> completedPAJobs, Instant now,
            ZoneId timezone) {
        if (now == null) {
            now = Instant.now();
        }
        if (timezone == null) {
            timezone = SchedulerConstants.DEFAULT_TIMEZONE;
        }

        Map<String, Long> quota = new HashMap<>(getTenantPaQuota(tenantId));
        Instant[] quotaInterval = getCurrentQuotaInterval(timezone, now);
        Instant quotaStartTime = quotaInterval[0];
        Instant quotaEndTime = quotaInterval[1];

        PAQuotaSummary summary = new PAQuotaSummary();
        summary.setQuotaResetTime(calculateResetTime(quotaEndTime, now));
        summary.setRecentlyCompletedPAs(getPAInQuotaInterval(completedPAJobs, quotaStartTime, quotaEndTime));

        // calculate remaining quota (can be negative due to manually started PAs but
        // set to 0 for display purposes)
        summary.getRecentlyCompletedPAs().forEach(jobSummary -> {
            if (StringUtils.isNotBlank(jobSummary.getConsumedQuotaName())) {
                String quotaName = jobSummary.getConsumedQuotaName();
                quota.put(quotaName, Math.max(quota.getOrDefault(quotaName, 0L) - 1, 0L));
            }
        });

        if (hasNonZeroQuota(summary.getRemainingPaQuota())) {
            String msg = String.format("This tenant is able to run %d schedule now PA and %d auto schedule PA today", //
                    summary.getRemainingPaQuota().get(QUOTA_SCHEDULE_NOW), //
                    summary.getRemainingPaQuota().get(QUOTA_AUTO_SCHEDULE));
            summary.setMessage(msg);
        } else {
            String msg = String.format("No PA quota left for this tenant. Wait for quota reset in %s.",
                    summary.getQuotaResetTime().getRemainingDuration());
            summary.setMessage(msg);
        }

        return summary;
    }

    private boolean hasNonZeroQuota(Map<String, Long> quota) {
        if (MapUtils.isEmpty(quota)) {
            return false;
        }

        return quota.values().stream().anyMatch(v -> v > 0);
    }

    private List<PAQuotaSummary.PASummary> getPAInQuotaInterval(List<WorkflowJob> workflowJobs, Instant quotaStartTime,
            Instant quotaEndTime) {
        if (CollectionUtils.isEmpty(workflowJobs)) {
            return Collections.emptyList();
        }

        return workflowJobs.stream() //
                .filter(Objects::nonNull) //
                .filter(filterJobStartedInCurrentInterval(quotaStartTime, quotaEndTime)) //
                .map(job -> {
                    PAQuotaSummary.PASummary summary = new PAQuotaSummary.PASummary();
                    summary.setApplicationId(job.getApplicationId());
                    Long startTime = getRootWorkflowStartTime(job);
                    if (startTime != null) {
                        summary.setRootJobStartedAt(Instant.ofEpochMilli(startTime));
                    }
                    // TODO set end time
                    summary.setConsumedQuotaName(getTagValue(job, WorkflowContextConstants.Tags.CONSUMED_QUOTA_NAME));
                    return summary;
                }) //
                .sorted(Comparator
                        .nullsLast(Comparator.comparing(PAQuotaSummary.PASummary::getRootJobStartedAt).reversed())) //
                .collect(Collectors.toList());
    }

    private PAQuotaSummary.QuotaResetTime calculateResetTime(Instant quotaEndTime, Instant now) {
        PAQuotaSummary.QuotaResetTime resetTime = new PAQuotaSummary.QuotaResetTime();
        resetTime.setResetAt(quotaEndTime);

        Duration timeUntilReset = Duration.between(now, quotaEndTime);
        long minutes = timeUntilReset.toMinutes();
        String hm = String.format("%02dh%02dm", minutes / 60, minutes);
        resetTime.setRemainingDuration(hm);
        return resetTime;
    }

    private Predicate<WorkflowJob> filterJobStartedInCurrentInterval(Instant quotaStartTime, Instant quotaEndTime) {
        return job -> {
            // completed within current quota period
            Long startTime = getRootWorkflowStartTime(job);
            if (startTime == null) {
                return false;
            }
            Instant startMoment = Instant.ofEpochMilli(startTime);
            return startMoment.isAfter(quotaStartTime) && startMoment.isBefore(quotaEndTime);
        };
    }

    // [ start time, end time ] of current quota interval
    private Instant[] getCurrentQuotaInterval(@NotNull ZoneId timezone, @NotNull Instant now) {
        Instant quotaStartTime = now.atZone(timezone).truncatedTo(ChronoUnit.DAYS).toInstant();
        Instant quotaEndTime = quotaStartTime.plus(1L, ChronoUnit.DAYS);
        return new Instant[] { quotaStartTime, quotaEndTime };
    }

    private Long getRootWorkflowStartTime(WorkflowJob job) {
        if (job == null || job.getWorkflowConfiguration() == null) {
            return null;
        }
        if (!job.getWorkflowConfiguration().isRestart()) {
            return job.getStartTimeInMillis();
        }

        String startTimeStr = getTagValue(job, WorkflowContextConstants.Tags.ROOT_WORKFLOW_START_TIME);
        if (StringUtils.isBlank(startTimeStr)) {
            return null;
        }
        try {
            return Long.parseLong(startTimeStr);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private String getTagValue(WorkflowJob job, @NotNull String tag) {
        Preconditions.checkNotNull(tag);
        if (job == null || job.getWorkflowConfiguration() == null) {
            return null;
        }
        return MapUtils.emptyIfNull(job.getWorkflowConfiguration().getTags()).get(tag);
    }
}
