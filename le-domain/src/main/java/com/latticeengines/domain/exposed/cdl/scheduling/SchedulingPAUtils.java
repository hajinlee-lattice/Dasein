package com.latticeengines.domain.exposed.cdl.scheduling;

import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.scheduling.queue.AutoScheduleSchedulingPAObject;
import com.latticeengines.domain.exposed.cdl.scheduling.queue.DataCloudRefreshSchedulingPAObject;
import com.latticeengines.domain.exposed.cdl.scheduling.queue.RetrySchedulingPAObject;
import com.latticeengines.domain.exposed.cdl.scheduling.queue.ScheduleNowSchedulingPAObject;
import com.latticeengines.domain.exposed.cdl.scheduling.queue.SchedulingPAObject;
import com.latticeengines.domain.exposed.cdl.scheduling.queue.SchedulingPAQueue;
import com.latticeengines.domain.exposed.security.TenantType;

public final class SchedulingPAUtils {

    protected SchedulingPAUtils() {
        throw new UnsupportedOperationException();
    }

    /*
     * helper to retrieve tenant ID from scheduling pa object
     */
    public static String getTenantId(SchedulingPAObject obj) {
        if (obj == null || obj.getTenantActivity() == null) {
            return null;
        }
        return obj.getTenantActivity().getTenantId();
    }

    /*
     * helper to transform list of scheduling pa objects into set of tenant ID
     */
    public static Set<String> getTenantIds(List<SchedulingPAObject> objs) {
        if (CollectionUtils.isEmpty(objs)) {
            return Collections.emptySet();
        }

        return objs.stream() //
                .map(SchedulingPAUtils::getTenantId) //
                .filter(Objects::nonNull) //
                .collect(Collectors.toSet());
    }

    /**
     * Calculate peace period that will stop auto scheduling PAs.
     *
     * @param now
     *            current time
     * @param timezone
     *            current timezone
     * @param autoScheduleQuota
     *            total quota for auto schedule PA
     * @return [ start time, end time ] of peace period, {@code null} if no such
     *         period
     */
    public static Pair<Instant, Instant> calculatePeacePeriod(@NotNull Instant now, @NotNull ZoneId timezone,
            long autoScheduleQuota) {
        Instant midnight = now.atZone(timezone).truncatedTo(ChronoUnit.DAYS).toInstant();
        // peace period always start at 6AM
        Instant startTime = midnight.plus(6L, ChronoUnit.HOURS);
        // max 12, decrease by 4 for extra quota more than 1
        long peacePeriodDurationInHr = 12L - Math.max(0, (autoScheduleQuota - 1)) * 4;
        if (peacePeriodDurationInHr <= 0) {
            return null;
        }
        Instant endTime = startTime.plus(peacePeriodDurationInHr, ChronoUnit.HOURS);
        return Pair.of(startTime, endTime);
    }

    /**
     * Determine whether current time is in peace period (stop auto schedule PA)
     * right now
     *
     * @param now
     *            current time
     * @param timezone
     *            current timezone
     * @param autoScheduleQuota
     *            total quota for auto schedule PA
     * @return true if in peace period
     */
    public static boolean isInPeacePeriod(@NotNull Instant now, @NotNull ZoneId timezone, long autoScheduleQuota) {
        Pair<Instant, Instant> peacePeriod = calculatePeacePeriod(now, timezone, autoScheduleQuota);
        if (peacePeriod == null) {
            // no peace period
            return false;
        }

        // [period start time, period end time]: both end inclusive
        return !(now.isBefore(peacePeriod.getLeft()) || now.isAfter(peacePeriod.getRight()));
    }

    public static List<SchedulingPAQueue> initQueue(SimulationContext simulationContext) {
        return initQueue(simulationContext.timeClock, simulationContext.systemStatus,
                simulationContext.getCanRunTenantActivity());
    }

    public static List<SchedulingPAQueue> initQueue(TimeClock schedulingPATimeClock, SystemStatus systemStatus,
                                              List<TenantActivity> tenantActivityList) {
        List<SchedulingPAQueue> schedulingPAQueues = new LinkedList<>();
        SchedulingPAQueue<RetrySchedulingPAObject> retrySchedulingPAQueue = new SchedulingPAQueue<>(systemStatus,
                RetrySchedulingPAObject.class, schedulingPATimeClock, true, "RetryQueue");
        SchedulingPAQueue<ScheduleNowSchedulingPAObject> scheduleNowSchedulingPAQueue = new SchedulingPAQueue<>(
                systemStatus, ScheduleNowSchedulingPAObject.class, schedulingPATimeClock, "customerScheduleNowQueue");
        SchedulingPAQueue<AutoScheduleSchedulingPAObject> autoScheduleSchedulingPAQueue = new SchedulingPAQueue<>(
                systemStatus, AutoScheduleSchedulingPAObject.class, schedulingPATimeClock,
                "customerAutoScheduleQueue");
        SchedulingPAQueue<DataCloudRefreshSchedulingPAObject> dataCloudRefreshSchedulingPAQueue = new SchedulingPAQueue<>(
                systemStatus, DataCloudRefreshSchedulingPAObject.class, schedulingPATimeClock,
                "customerDataCloudRefreshQueue");
        SchedulingPAQueue<ScheduleNowSchedulingPAObject> nonCustomerScheduleNowSchedulingPAQueue = new SchedulingPAQueue<>(
                systemStatus, ScheduleNowSchedulingPAObject.class, schedulingPATimeClock, "nonCustomerScheduleNowQueue");
        SchedulingPAQueue<AutoScheduleSchedulingPAObject> nonCustomerAutoScheduleSchedulingPAQueue = new SchedulingPAQueue<>(
                systemStatus, AutoScheduleSchedulingPAObject.class, schedulingPATimeClock, "nonCustomerAutoScheduleQueue");
        SchedulingPAQueue<DataCloudRefreshSchedulingPAObject> nonDataCloudRefreshSchedulingPAQueue = new SchedulingPAQueue<>(
                systemStatus, DataCloudRefreshSchedulingPAObject.class, schedulingPATimeClock,
                "nonCustomerDataCloudRefreshQueue");
        for (TenantActivity tenantActivity : tenantActivityList) {
            RetrySchedulingPAObject retrySchedulingPAObject = new RetrySchedulingPAObject(tenantActivity);
            ScheduleNowSchedulingPAObject scheduleNowSchedulingPAObject = new ScheduleNowSchedulingPAObject(
                    tenantActivity);
            AutoScheduleSchedulingPAObject autoScheduleSchedulingPAObject = new AutoScheduleSchedulingPAObject(
                    tenantActivity);
            DataCloudRefreshSchedulingPAObject dataCloudRefreshSchedulingPAObject = new DataCloudRefreshSchedulingPAObject(
                    tenantActivity);
            retrySchedulingPAQueue.add(retrySchedulingPAObject);
            if (tenantActivity.getTenantType() == TenantType.CUSTOMER) {
                scheduleNowSchedulingPAQueue.add(scheduleNowSchedulingPAObject);
                autoScheduleSchedulingPAQueue.add(autoScheduleSchedulingPAObject);
                dataCloudRefreshSchedulingPAQueue.add(dataCloudRefreshSchedulingPAObject);
            } else {
                nonCustomerScheduleNowSchedulingPAQueue.add(scheduleNowSchedulingPAObject);
                nonCustomerAutoScheduleSchedulingPAQueue.add(autoScheduleSchedulingPAObject);
                nonDataCloudRefreshSchedulingPAQueue.add(dataCloudRefreshSchedulingPAObject);
            }
        }
        schedulingPAQueues.add(retrySchedulingPAQueue);
        schedulingPAQueues.add(scheduleNowSchedulingPAQueue);
        schedulingPAQueues.add(autoScheduleSchedulingPAQueue);
        schedulingPAQueues.add(nonCustomerScheduleNowSchedulingPAQueue);
        schedulingPAQueues.add(dataCloudRefreshSchedulingPAQueue);
        schedulingPAQueues.add(nonCustomerAutoScheduleSchedulingPAQueue);
        schedulingPAQueues.add(nonDataCloudRefreshSchedulingPAQueue);
        return schedulingPAQueues;
    }
}
