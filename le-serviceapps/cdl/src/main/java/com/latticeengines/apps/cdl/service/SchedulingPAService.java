package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.domain.exposed.cdl.SchedulingPAQueue;

public interface SchedulingPAService {

    Map<String, Object> setSystemStatus();

    List<SchedulingPAQueue> initQueue();

    Map<String, Set<String>> getCanRunJobTenantList();

    Map<String, List<String>> showQueue();

    String getPositionFromQueue(String tenantName);
}
