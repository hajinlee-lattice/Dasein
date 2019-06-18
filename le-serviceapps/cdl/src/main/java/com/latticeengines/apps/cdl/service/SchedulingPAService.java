package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.domain.exposed.cdl.SchedulingPAQueue;
import com.latticeengines.domain.exposed.cdl.SystemStatus;
import com.latticeengines.domain.exposed.cdl.TenantActivity;

public interface SchedulingPAService {

    SystemStatus setSystemStatus(List<TenantActivity> tenantActivityList);

    List<SchedulingPAQueue> initQueue();

    Map<String, Set<String>> getCanRunJobTenantList();

    String showQueue();

    String getPositionFromQueue(String tenantName);
}
