package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;

public interface PriorityQueueService {

    void init();

    Boolean isLargeTenant(String tenantName);

    List<String> getRunningPATenantId();

    String peekFromRetryPriorityQueue();

    String peekFromCustomerScheduleNowPriorityQueue();

    String peekFromCustomerAutoSchedulePriorityQueue();

    String peekFromCustomerDataCloudRefreshPriorityQueue();

    String peekFromNonCustomerScheduleNowPriorityQueue();

    String peekFromNonCustomerAutoSchedulePriorityQueue();

    String peekFromNonCustomerDataCloudRefreshPriorityQueue();

    String pollFromRetryPriorityQueue();

    String pollFromCustomerScheduleNowPriorityQueue();

    String pollFromCustomerAutoSchedulePriorityQueue();

    String pollFromCustomerDataCloudRefreshPriorityQueue();

    String pollFromNonCustomerScheduleNowPriorityQueue();

    String pollFromNonCustomerAutoSchedulePriorityQueue();

    String pollFromNonCustomerDataCloudRefreshPriorityQueue();

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
