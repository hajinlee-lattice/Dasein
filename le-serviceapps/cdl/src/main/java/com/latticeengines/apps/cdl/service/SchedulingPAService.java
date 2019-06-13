package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;

public interface SchedulingPAService {

    void setSystemStatus();

    void initQueue();

    Boolean isLargeTenant(String tenantName);

    List<String> getRunningPATenantId();

    List<String> getCanRunJobTenantFromRetryPriorityQueue();

    List<String> getCanRunJobTenantFromCustomerScheduleNowPriorityQueue();

    List<String> getCanRunJobTenantFromCustomerAutoSchedulePriorityQueue();

    List<String> getCanRunJobTenantFromCustomerDataCloudRefreshPriorityQueue();

    List<String> getCanRunJobTenantFromNonCustomerScheduleNowPriorityQueue();

    List<String> getCanRunJobTenantFromNonCustomerAutoSchedulePriorityQueue();

    List<String> getCanRunJobTenantFromNonCustomerDataCloudRefreshPriorityQueue();

    Map<String, List<String>> showQueue();

    String getPositionFromQueue(String tenantName);
}
