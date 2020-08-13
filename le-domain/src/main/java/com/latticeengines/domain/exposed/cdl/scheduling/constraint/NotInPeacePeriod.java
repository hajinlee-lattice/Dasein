package com.latticeengines.domain.exposed.cdl.scheduling.constraint;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

import org.apache.commons.lang3.ObjectUtils;

import com.latticeengines.domain.exposed.cdl.scheduling.SchedulerConstants;
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
        Instant midnight = now.atZone(timezone).truncatedTo(ChronoUnit.DAYS).toInstant();
        Instant sixAM = midnight.plus(6L, ChronoUnit.HOURS);
        Instant sixPM = sixAM.plus(12L, ChronoUnit.HOURS);
        Instant oldImportThreshold = now.minus(OLD_IMPORT_THRESHOLD_IN_HR, ChronoUnit.HOURS);
        // TODO maybe shorten peace period if quota is more than 1
        if (now.isBefore(sixAM) || now.isAfter(sixPM)) {
            // not in peace period
            return ConstraintValidationResult.VALID;
        } else if (firstAction != null && firstAction.isBefore(oldImportThreshold)) {
            // have old import action, should allow scheduling PA
            String msg = String.format("tenant is in peace period, but contains import older than %d hours",
                    OLD_IMPORT_THRESHOLD_IN_HR);
            return new ConstraintValidationResult(false, msg);
        }

        Duration timeUntilPeacePeriodEnd = Duration.between(now, sixPM);
        long minutes = timeUntilPeacePeriodEnd.toMinutes();
        String hm = String.format("%02dh%02dm", minutes / 60, (minutes % 60));
        String msg = String.format("currently in peace period (which ends in %s)", hm);
        return new ConstraintValidationResult(true, msg);
    }
}
