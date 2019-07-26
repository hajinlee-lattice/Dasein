package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.security.TenantType;

public class SchedulingPAUtil {

    /*
     * helper to retrieve tenant ID from scheduling pa object
     */
    public static String getTenantId(SchedulingPAObject obj) {
        if (obj == null || obj.getTenantActivity() == null) {
            return null;
        }
        return obj.getTenantActivity().getTenantId();
    }

    /*
     * helper to transform list of scheduling pa objects into set of tenant ID
     */
    public static Set<String> getTenantIds(List<SchedulingPAObject> objs) {
        if (CollectionUtils.isEmpty(objs)) {
            return Collections.emptySet();
        }

        return objs.stream() //
                .map(SchedulingPAUtil::getTenantId) //
                .filter(Objects::nonNull) //
                .collect(Collectors.toSet());
    }

    public static List<SchedulingPAQueue> initQueue(SimulationContext simulationContext) {
        return initQueue(simulationContext.timeClock, simulationContext.systemStatus,
                simulationContext.getCanRunTenantActivity());
    }

    public static List<SchedulingPAQueue> initQueue(TimeClock schedulingPATimeClock, SystemStatus systemStatus,
                                              List<TenantActivity> tenantActivityList) {
        List<SchedulingPAQueue> schedulingPAQueues = new LinkedList<>();
        SchedulingPAQueue<RetrySchedulingPAObject> retrySchedulingPAQueue = new SchedulingPAQueue<>(systemStatus,
                RetrySchedulingPAObject.class, schedulingPATimeClock, true, "RetryQueue");
        SchedulingPAQueue<ScheduleNowSchedulingPAObject> scheduleNowSchedulingPAQueue = new SchedulingPAQueue<>(
                systemStatus, ScheduleNowSchedulingPAObject.class, schedulingPATimeClock, "customerScheduleNowQueue");
        SchedulingPAQueue<AutoScheduleSchedulingPAObject> autoScheduleSchedulingPAQueue = new SchedulingPAQueue<>(
                systemStatus, AutoScheduleSchedulingPAObject.class, schedulingPATimeClock,
                "customerAutoScheduleQueue");
        SchedulingPAQueue<DataCloudRefreshSchedulingPAObject> dataCloudRefreshSchedulingPAQueue = new SchedulingPAQueue<>(
                systemStatus, DataCloudRefreshSchedulingPAObject.class, schedulingPATimeClock,
                "customerDataCloudRefreshQueue");
        SchedulingPAQueue<ScheduleNowSchedulingPAObject> nonCustomerScheduleNowSchedulingPAQueue = new SchedulingPAQueue<>(
                systemStatus, ScheduleNowSchedulingPAObject.class, schedulingPATimeClock, "nonCustomerScheduleNowQueue");
        SchedulingPAQueue<AutoScheduleSchedulingPAObject> nonCustomerAutoScheduleSchedulingPAQueue = new SchedulingPAQueue<>(
                systemStatus, AutoScheduleSchedulingPAObject.class, schedulingPATimeClock, "nonCustomerAutoScheduleQueue");
        SchedulingPAQueue<DataCloudRefreshSchedulingPAObject> nonDataCloudRefreshSchedulingPAQueue = new SchedulingPAQueue<>(
                systemStatus, DataCloudRefreshSchedulingPAObject.class, schedulingPATimeClock,
                "nonCustomerDataCloudRefreshQueue");
        for (TenantActivity tenantActivity : tenantActivityList) {
            RetrySchedulingPAObject retrySchedulingPAObject = new RetrySchedulingPAObject(tenantActivity);
            ScheduleNowSchedulingPAObject scheduleNowSchedulingPAObject = new ScheduleNowSchedulingPAObject(
                    tenantActivity);
            AutoScheduleSchedulingPAObject autoScheduleSchedulingPAObject = new AutoScheduleSchedulingPAObject(
                    tenantActivity);
            DataCloudRefreshSchedulingPAObject dataCloudRefreshSchedulingPAObject = new DataCloudRefreshSchedulingPAObject(
                    tenantActivity);
            retrySchedulingPAQueue.add(retrySchedulingPAObject);
            if (tenantActivity.getTenantType() == TenantType.CUSTOMER) {
                scheduleNowSchedulingPAQueue.add(scheduleNowSchedulingPAObject);
                autoScheduleSchedulingPAQueue.add(autoScheduleSchedulingPAObject);
                dataCloudRefreshSchedulingPAQueue.add(dataCloudRefreshSchedulingPAObject);
            } else {
                nonCustomerScheduleNowSchedulingPAQueue.add(scheduleNowSchedulingPAObject);
                nonCustomerAutoScheduleSchedulingPAQueue.add(autoScheduleSchedulingPAObject);
                nonDataCloudRefreshSchedulingPAQueue.add(dataCloudRefreshSchedulingPAObject);
            }
        }
        schedulingPAQueues.add(retrySchedulingPAQueue);
        schedulingPAQueues.add(scheduleNowSchedulingPAQueue);
        schedulingPAQueues.add(autoScheduleSchedulingPAQueue);
        schedulingPAQueues.add(dataCloudRefreshSchedulingPAQueue);
        schedulingPAQueues.add(nonCustomerScheduleNowSchedulingPAQueue);
        schedulingPAQueues.add(nonCustomerAutoScheduleSchedulingPAQueue);
        schedulingPAQueues.add(nonDataCloudRefreshSchedulingPAQueue);
        return schedulingPAQueues;
    }
}
