package com.latticeengines.domain.exposed.cdl.scheduling;

import java.time.ZoneId;

/**
 * Constants related to PA scheduler functionality
 */
public class SchedulerConstants {
    /*-
     * quota names
     */
    public static final String QUOTA_SCHEDULE_NOW = "ScheduleNow";
    public static final String QUOTA_AUTO_SCHEDULE = "AutoSchedule";

    public static final ZoneId DEFAULT_TIMEZONE = ZoneId.of("UTC");
}
