package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingPAQueue;

public interface SchedulingPAService {

    Map<String, Object> setSystemStatus(@NotNull String schedulerName);

    List<SchedulingPAQueue> initQueue(@NotNull String schedulerName);

    Map<String, Set<String>> getCanRunJobTenantList(@NotNull String schedulerName);

    Map<String, List<String>> showQueue(@NotNull String schedulerName);

    String getPositionFromQueue(@NotNull String schedulerName, String tenantName);

    boolean isSchedulerEnabled(@NotNull String schedulerName);
}
