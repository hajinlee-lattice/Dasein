package com.latticeengines.domain.exposed.cdl.scheduling;

import java.time.ZoneId;

/**
 * Constants related to PA scheduler functionality
 */
public final class SchedulerConstants {
    /*-
     * quota names
     */
    public static final String QUOTA_SCHEDULE_NOW = "ScheduleNow";
    public static final String QUOTA_AUTO_SCHEDULE = "AutoSchedule";

    public static final ZoneId DEFAULT_TIMEZONE = ZoneId.of("UTC");
    public static final long RECENT_PA_LOOK_BACK_DAYS = 2L;

    protected SchedulerConstants() {
        throw new UnsupportedOperationException();
    }
}
