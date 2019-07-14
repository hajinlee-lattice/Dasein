package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.SchedulingPAService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.apps.cdl.util.Simulation;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.scheduling.SimulationTimeClock;
import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.event.DataCloudRefreshEvent;
import com.latticeengines.domain.exposed.cdl.scheduling.event.Event;
import com.latticeengines.domain.exposed.cdl.scheduling.event.ImportActionEvent;
import com.latticeengines.domain.exposed.cdl.scheduling.event.ScheduleNowEvent;
import com.latticeengines.domain.exposed.cdl.scheduling.event.SchedulingEvent;
import com.latticeengines.domain.exposed.security.TenantType;

public class SimulationUnitTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SimulationUnitTestNG.class);

    @Inject
    private SchedulingPAService schedulingPAService;

    private PriorityQueue<Event> priorityQueue = new PriorityQueue<>();
    private SimulationTimeClock clock = new SimulationTimeClock();
    private List<String> dataCloudRefreshTenant;
    private List<String> tenantList;

    @Test(groups = "unit")
    public void testMain() {
        this.tenantList = initTenant();
        this.clock.setTimestamp(1531373313);
        Map<String, TenantActivity> tenantActivityMap = setTenantInitState(tenantList);
        this.priorityQueue.addAll(generateTenantEvents());
        this.priorityQueue.addAll(generateSchedulingEvent());
        this.priorityQueue.addAll(generateDataCloudRefreshEvent());
        SystemStatus systemStatus = newStatus(5, 10, 2);
        Simulation simulation = new Simulation(schedulingPAService, systemStatus, tenantList, tenantActivityMap,
                priorityQueue, clock);
        simulation.run();
    }

    private SystemStatus newStatus(int scheduleNowLimit, int totalJobLimit, int largeJobLimit) {
        SystemStatus status = new SystemStatus();
        status.setLargeJobTenantId(new HashSet<>());
        status.setRunningPATenantId(new HashSet<>());
        status.setCanRunScheduleNowJobCount(scheduleNowLimit);
        status.setCanRunJobCount(totalJobLimit);
        status.setCanRunLargeJobCount(largeJobLimit);
        return status;
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
            time += i * 5 * 60; // 5 min
            events.add(new SchedulingEvent(time));
        }
        return events;
    }

    private List<Event> generateDataCloudRefreshEvent() {
        List<Event> events = new ArrayList<>();
        Random r = new Random();
        long time = clock.getCurrentTime();
        for (String tenantId : dataCloudRefreshTenant) {
            time += r.nextInt(1000) * 60;
            events.add(new DataCloudRefreshEvent(tenantId, time));
        }
        return events;
    }

    private long getRandomTime() {
        Random r = new Random();
        int randomInt = r.nextInt(100) * 60;
        long time = clock.getCurrentTime() + randomInt;
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
            tenantActivity.setLarge(false);
            if (index % 2 == 0) {
                tenantActivity.setTenantType(TenantType.CUSTOMER);
            } else {
                tenantActivity.setTenantType(TenantType.QA);
            }
            if (index % 5 == 0) {
                dataCloudRefreshTenant.add(tenant);
            }
            if (index % 3 == 1) {
                tenantActivity.setLarge(true);
            }
            canRunTenantActivityMap.put(tenant, tenantActivity);
            index++;
        }
        log.info("dataCloudRefreshTenant is : " + JsonUtils.serialize(dataCloudRefreshTenant));
        return canRunTenantActivityMap;
    }

}
