package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingPAQueue;
import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;

public interface SchedulingPAService {

    Map<String, Object> setSystemStatus();

    List<SchedulingPAQueue> initQueue();

    List<SchedulingPAQueue> initQueue(TimeClock schedulingPATimeClock, SystemStatus systemStatus,
                                      List<TenantActivity> tenantActivityList);

    Map<String, Set<String>> getCanRunJobTenantList();

    Map<String, List<String>> showQueue();

    String getPositionFromQueue(String tenantName);
}
