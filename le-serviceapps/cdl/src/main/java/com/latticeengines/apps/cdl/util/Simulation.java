package com.latticeengines.apps.cdl.util;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.scheduling.AutoScheduleSchedulingPAObject;
import com.latticeengines.domain.exposed.cdl.scheduling.DataCloudRefreshSchedulingPAObject;
import com.latticeengines.domain.exposed.cdl.scheduling.RetrySchedulingPAObject;
import com.latticeengines.domain.exposed.cdl.scheduling.ScheduleNowSchedulingPAObject;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingPAQueue;
import com.latticeengines.domain.exposed.cdl.scheduling.SimulationStats;
import com.latticeengines.domain.exposed.cdl.scheduling.SimulationTimeClock;
import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;
import com.latticeengines.domain.exposed.cdl.scheduling.event.Event;
import com.latticeengines.domain.exposed.cdl.scheduling.event.PAEndEvent;
import com.latticeengines.domain.exposed.cdl.scheduling.event.PAStartEvent;
import com.latticeengines.domain.exposed.cdl.scheduling.event.SchedulingEvent;
import com.latticeengines.domain.exposed.security.TenantType;

public class Simulation {

    private static final Logger log = LoggerFactory.getLogger(Simulation.class);

    private SimulationStats simulationStats;
    private PriorityQueue<Event> priorityQueue;
    private SimulationTimeClock clock;
    private SystemStatus systemStatus;
    // FIXME remove these
    private Map<String, Integer> startedPAs = new HashMap<>();
    private Map<String, Integer> finishedPAs = new HashMap<>();

    public Simulation(SystemStatus systemStatus, List<String> tenantList, Set<String> dcRefreshTenants,
            Map<String, TenantActivity> tenantInitState, PriorityQueue<Event> priorityQueue,
            SimulationTimeClock timeClock) {
        this.clock = timeClock;
        this.priorityQueue = priorityQueue;
        this.systemStatus = systemStatus;
        this.simulationStats = new SimulationStats(tenantList, dcRefreshTenants, tenantInitState);
        this.simulationStats.setTimeClock(clock);

    }

    public void run() {
        while (!priorityQueue.isEmpty()) {
            Event e = priorityQueue.poll();
            // fake current time
            clock.setTimestamp(e.getTime());
            if (e.getClass().equals(SchedulingEvent.class)) {
                setSchedulingPAQueues();
                this.simulationStats.printSummary();
            }
            List<Event> events = e.changeState(systemStatus, simulationStats);
            if (!CollectionUtils.isEmpty(events)) {
                priorityQueue.addAll(events);
            }
            log.info("current event is : " + JsonUtils.serialize(e));

            // FIXME remove these
            if (e instanceof PAStartEvent) {
                startedPAs.put(e.getTenantId(), startedPAs.getOrDefault(e.getTenantId(), 0) + 1);
            } else if (e instanceof PAEndEvent) {
                finishedPAs.put(e.getTenantId(), finishedPAs.getOrDefault(e.getTenantId(), 0) + 1);
            }
        }
        log.info(JsonUtils.serialize(this.systemStatus));
        // FIXME remove these
        System.out.println(startedPAs.size() + ", " + startedPAs);
        System.out.println(finishedPAs.size() + ", " + finishedPAs);
    }

    // TODO move to Scheduling event, remove reference schedulingPAQueues in
    // SimulationStats
    public void setSchedulingPAQueues() {
        log.info(JsonUtils.serialize(simulationStats.getCanRunTenantActivity()));
        List<SchedulingPAQueue> schedulingPAQueues = initQueue(clock, systemStatus,
                this.simulationStats.getCanRunTenantActivity());
        this.simulationStats.setSchedulingPAQueues(schedulingPAQueues);
    }

    // FIXME remove (move to util)
    private List<SchedulingPAQueue> initQueue(TimeClock schedulingPATimeClock, SystemStatus systemStatus,
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
