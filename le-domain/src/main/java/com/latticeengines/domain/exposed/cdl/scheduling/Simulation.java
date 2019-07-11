package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.scheduling.event.DataCloudRefreshEvent;
import com.latticeengines.domain.exposed.cdl.scheduling.event.Event;
import com.latticeengines.domain.exposed.cdl.scheduling.event.ImportActionEvent;
import com.latticeengines.domain.exposed.cdl.scheduling.event.ScheduleNowEvent;
import com.latticeengines.domain.exposed.cdl.scheduling.event.SchedulingEvent;
import com.latticeengines.domain.exposed.security.TenantType;

public class Simulation {

    private static final Logger log = LoggerFactory.getLogger(Simulation.class);

    private SimulationStats simulationStats;
    private PriorityQueue<Event> priorityQueue = new PriorityQueue<>();
    private SimulationTimeClock clock;
    private List<String> dataCloudRefreshTenant;
    private List<String> tenantList;
    private SystemStatus systemStatus;

    public Simulation(SystemStatus systemStatus) {
        this.clock = new SimulationTimeClock();
        this.systemStatus = systemStatus;
        this.tenantList = initTenant();
        this.simulationStats = new SimulationStats(tenantList, setTenantInitState(tenantList));
        this.simulationStats.setTimeClock(clock);
        this.priorityQueue.addAll(generateTenantEvents());
        this.priorityQueue.addAll(generateSchedulingEvent());
        this.priorityQueue.addAll(generateDataCloudRefreshEvent());
    }

    public void run() {
        while (!priorityQueue.isEmpty()) {
            Event e = priorityQueue.poll();
            // fake current time
            clock.setTimestamp(e.getTime());
            priorityQueue.addAll(e.changeState(systemStatus, simulationStats));
        }
        this.simulationStats.printSummary();
        log.info(JsonUtils.serialize(this.systemStatus));
    }

    public void setSchedulingPAQueues(List<SchedulingPAQueue> schedulingPAQueues) {
        this.simulationStats.setSchedulingPAQueues(schedulingPAQueues);
    }

    public List<TenantActivity> getActivityList() {
        return this.simulationStats.getCanRunTenantActivity();
    }

    public TimeClock getTimeClock() {
        return this.clock;
    }

    private List<Event> generateTenantEvents() {
        List<Event> eventList = new ArrayList<>();
        Random r = new Random();
        for (String tenantId : tenantList) {
            int count = r.nextInt(5) + 1;
            for (int i = 0; i < count; i++) {
                int type = r.nextInt(5);
                if (type < 1) {
                    ScheduleNowEvent scheduleNowEvent = new ScheduleNowEvent(tenantId, getRandomTime());
                    eventList.add(scheduleNowEvent);
                } else {
                    ImportActionEvent importActionEvent = new ImportActionEvent(tenantId, getRandomTime());
                    eventList.add(importActionEvent);
                }
            }
        }
        return eventList;
    }

    private List<Event> generateSchedulingEvent() {
        long time = clock.getCurrentTime();
        List<Event> events = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            time += 5 * 60 * 1000; // 5 min
            events.add(new SchedulingEvent(time));
        }
        return events;
    }

    private List<Event> generateDataCloudRefreshEvent() {
        List<Event> events = new ArrayList<>();
        Random r = new Random();
        long time = clock.getCurrentTime();
        for (String tenantId : dataCloudRefreshTenant) {
            time += r.nextInt(1000) * 3600;
            events.add(new DataCloudRefreshEvent(tenantId, time));
        }
        return events;
    }

    private long getRandomTime() {
        Random r = new Random();
        long time = clock.getCurrentTime() + r.nextInt(100000) * 3600;
        return time;
    }

    private List<String> initTenant() {
        List<String> tenantList = new LinkedList<>();
        for (int i = 1; i < 21; i ++) {
            tenantList.add("testTenant" + i);
        }
        return tenantList;
    }

    private Map<String, TenantActivity> setTenantInitState(List<String> tenantList) {
        Map<String, TenantActivity> canRunTenantActivityMap = new HashMap<>();
        dataCloudRefreshTenant = new ArrayList<>();
        int index = 0;
        Random r = new Random();
        for (String tenant : tenantList) {
            TenantActivity tenantActivity = new TenantActivity();
            tenantActivity.setTenantId(tenant);
            if (index % 2 == 0) {
                tenantActivity.setTenantType(TenantType.CUSTOMER);
            } else {
                tenantActivity.setTenantType(TenantType.QA);
            }
            if (index % 5 == 0) {
                dataCloudRefreshTenant.add(tenant);
            }
            canRunTenantActivityMap.put(tenant, tenantActivity);
            index++;
        }
        log.info("dataCloudRefreshTenant is : " + JsonUtils.serialize(dataCloudRefreshTenant));
        return canRunTenantActivityMap;
    }
}
