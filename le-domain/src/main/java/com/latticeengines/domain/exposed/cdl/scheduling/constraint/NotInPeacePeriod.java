package com.latticeengines.domain.exposed.cdl.scheduling.constraint;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulerConstants;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingPAUtils;
import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;

public class NotInPeacePeriod implements Constraint {
    private static final long OLD_IMPORT_THRESHOLD_IN_HR = 12L;

    @Override
    public ConstraintValidationResult validate(SystemStatus currentState, TenantActivity target,
            TimeClock timeClock) {
        ZoneId timezone = ObjectUtils.defaultIfNull(target.getTimezone(), SchedulerConstants.DEFAULT_TIMEZONE);
        Instant now = Instant.ofEpochMilli(timeClock.getCurrentTime());

        Instant firstAction = target.getFirstIngestActionTime() == null ? null : Instant.ofEpochMilli(target.getFirstIngestActionTime());
        Instant oldImportThreshold = now.minus(OLD_IMPORT_THRESHOLD_IN_HR, ChronoUnit.HOURS);
        long autoScheduleQuota = getAutoScheduleQuota(target);
        if (!SchedulingPAUtils.isInPeacePeriod(now, timezone, autoScheduleQuota)) {
            // not in peace period
            return ConstraintValidationResult.VALID;
        } else if (firstAction != null && firstAction.isBefore(oldImportThreshold)) {
            // have old import action, should allow scheduling PA
            String msg = String.format("tenant is in peace period, but contains import older than %d hours",
                    OLD_IMPORT_THRESHOLD_IN_HR);
            return new ConstraintValidationResult(false, msg);
        }

        Pair<Instant, Instant> period = SchedulingPAUtils.calculatePeacePeriod(now, timezone, autoScheduleQuota);
        Preconditions.checkNotNull(period, "should have peace period when reaching here");
        Duration timeUntilPeacePeriodEnd = Duration.between(now, period.getRight());
        long minutes = timeUntilPeacePeriodEnd.toMinutes();
        String hm = String.format("%02dh%02dm", minutes / 60, (minutes % 60));
        String msg = String.format("currently in peace period (which ends in %s)", hm);
        return new ConstraintValidationResult(true, msg);
    }

    private long getAutoScheduleQuota(@NotNull TenantActivity activity) {
        return MapUtils.emptyIfNull(activity.getTotalPaQuota()).getOrDefault(SchedulerConstants.QUOTA_AUTO_SCHEDULE,
                0L);
    }
}
