package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingPAQueue;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingResult;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingStatus;

public interface SchedulingPAService {

    String ACTIVE_STACK_SCHEDULER_NAME = "active";
    String INACTIVE_STACK_SCHEDULER_NAME = "inactive";

    Map<String, Object> setSystemStatus(@NotNull String schedulerName);

    List<SchedulingPAQueue> initQueue(@NotNull String schedulerName);

    SchedulingResult getSchedulingResult(@NotNull String schedulerName);

    Map<String, List<String>> showQueue(@NotNull String schedulerName);

    String getPositionFromQueue(@NotNull String schedulerName, String tenantName);

    boolean isSchedulerEnabled(@NotNull String schedulerName);

    /**
     * Retrieve all scheduler-related information for a specific tenant
     */
    SchedulingStatus getSchedulingStatus(@NotNull String customerSpace, @NotNull String schedulerName);
}
