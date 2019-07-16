package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.security.TenantType;

public class SchedulingPAUtil {

    private static final Logger log = LoggerFactory.getLogger(SchedulingPAUtil.class);

    public static List<SchedulingPAQueue> initQueue(SimulationContext simulationContext) {
        return initQueue(simulationContext.timeClock, simulationContext.systemStatus,
                simulationContext.getCanRunTenantActivity());
    }

    public static List<SchedulingPAQueue> initQueue(TimeClock schedulingPATimeClock, SystemStatus systemStatus,
                                              List<TenantActivity> tenantActivityList) {
        List<SchedulingPAQueue> schedulingPAQueues = new LinkedList<>();
        SchedulingPAQueue<RetrySchedulingPAObject> retrySchedulingPAQueue = new SchedulingPAQueue<>(systemStatus,
                RetrySchedulingPAObject.class, schedulingPATimeClock, true);
        SchedulingPAQueue<ScheduleNowSchedulingPAObject> scheduleNowSchedulingPAQueue = new SchedulingPAQueue<>(
                systemStatus, ScheduleNowSchedulingPAObject.class, schedulingPATimeClock);
        SchedulingPAQueue<AutoScheduleSchedulingPAObject> autoScheduleSchedulingPAQueue = new SchedulingPAQueue<>(
                systemStatus, AutoScheduleSchedulingPAObject.class, schedulingPATimeClock);
        SchedulingPAQueue<DataCloudRefreshSchedulingPAObject> dataCloudRefreshSchedulingPAQueue = new SchedulingPAQueue<>(
                systemStatus, DataCloudRefreshSchedulingPAObject.class, schedulingPATimeClock);
        SchedulingPAQueue<ScheduleNowSchedulingPAObject> nonCustomerScheduleNowSchedulingPAQueue = new SchedulingPAQueue<>(
                systemStatus, ScheduleNowSchedulingPAObject.class, schedulingPATimeClock);
        SchedulingPAQueue<AutoScheduleSchedulingPAObject> nonCustomerAutoScheduleSchedulingPAQueue = new SchedulingPAQueue<>(
                systemStatus, AutoScheduleSchedulingPAObject.class, schedulingPATimeClock);
        SchedulingPAQueue<DataCloudRefreshSchedulingPAObject> nonDataCloudRefreshSchedulingPAQueue = new SchedulingPAQueue<>(
                systemStatus, DataCloudRefreshSchedulingPAObject.class, schedulingPATimeClock);
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
        log.info(JsonUtils.serialize(scheduleNowSchedulingPAQueue));
        return schedulingPAQueues;
    }
}
