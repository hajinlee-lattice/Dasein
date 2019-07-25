package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.util.Simulation;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.scheduling.SimulationTenant;
import com.latticeengines.domain.exposed.cdl.scheduling.SimulationTimeClock;
import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.event.Event;
import com.latticeengines.domain.exposed.cdl.scheduling.event.ImportActionEvent;
import com.latticeengines.domain.exposed.cdl.scheduling.event.ScheduleNowEvent;
import com.latticeengines.domain.exposed.security.TenantType;

public class SimulationUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(SimulationUnitTestNG.class);

    private PriorityQueue<Event> priorityQueue = new PriorityQueue<>();
    private SimulationTimeClock clock = new SimulationTimeClock();
    private List<String> dataCloudRefreshTenant;
    private List<String> tenantList;

    @Test(groups = "unit")
    public void testMain() {
        this.tenantList = initTenant();
        this.clock.setTimestamp(1531373313L * 1000);
        Map<String, SimulationTenant> simulationTenantMap = setTenantInitState(tenantList);
        String duringTime = "2m";
        this.priorityQueue.addAll(generateTenantEvents(duringTime));
        SystemStatus systemStatus = newStatus(5, 10, 2);
        Simulation simulation = new Simulation(systemStatus, new HashSet<>(dataCloudRefreshTenant),
                simulationTenantMap, priorityQueue, clock, duringTime);
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

    private List<Event> generateTenantEvents(String duringTime) {
        List<Event> eventList = new ArrayList<>();
        Random r = new Random();
        for (String tenantId : tenantList) {
            int count = r.nextInt(transferCount(duringTime) * 20) + 1; // 1 week create avg 10 event per tenant
            for (int i = 0; i < count; i++) {
                int type = r.nextInt(5);
                if (type < 1) {
                    ScheduleNowEvent scheduleNowEvent = new ScheduleNowEvent(tenantId, getRandomTime(duringTime));
                    eventList.add(scheduleNowEvent);
                } else {
                    ImportActionEvent importActionEvent = new ImportActionEvent(tenantId, getRandomTime(duringTime));
                    eventList.add(importActionEvent);
                }
            }
        }
        return eventList;
    }

    private long getRandomTime(String duringTime) {
        Random r = new Random();
        int randomInt = r.nextInt(transferTime(duringTime)); // 2 day
        return clock.getCurrentTime() + (long) randomInt * 1000;
    }

    private List<String> initTenant() {
        List<String> tenantList = new LinkedList<>();
        for (int i = 1; i < 21; i++) {
            tenantList.add("testTenant" + i);
        }
        return tenantList;
    }

    private Map<String, SimulationTenant> setTenantInitState(List<String> tenantList) {
        Map<String, SimulationTenant> simulationTenantMap = new HashMap<>();
        dataCloudRefreshTenant = new ArrayList<>();
        int index = 0;
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
            tenantActivity.setAutoSchedule(true);
            tenantActivity.setInvokeTime(clock.getCurrentTime());
            simulationTenantMap.put(tenant, new SimulationTenant(tenantActivity));
            index++;
        }
        log.info("dataCloudRefreshTenant is : " + JsonUtils.serialize(dataCloudRefreshTenant));
        return simulationTenantMap;
    }

    private int transferTime(String duringTime) {//get the duringTimeMillSecond
        String timeNumber = duringTime.substring(0, duringTime.length() - 1);
        String tag = duringTime.substring(duringTime.length() - 1);
        long time = 0L;
        switch (tag) {
            case "d" : time = 86400L * 1000 * Long.valueOf(timeNumber); break;
            case "w" : time = 7 * 86400L * 1000 * Long.valueOf(timeNumber); break;
            case "m" : time = 30 * 7 * 86400L * 1000 * Long.valueOf(timeNumber); break;
            default: break;
        }
        return (int) (time / 1000);
    }

    private int transferCount(String duringTime) {
        String timeNumber = duringTime.substring(0, duringTime.length() - 1);
        String tag = duringTime.substring(duringTime.length() - 1);
        int count = 0;
        switch (tag) {
            case "d" : count = Integer.valueOf(timeNumber) / 7; break;
            case "w" : count = Integer.valueOf(timeNumber); break;
            case "m" : count = 4 * Integer.valueOf(timeNumber); break;//set 1m = 4w, so we can get the week count
            default: break;
        }
        return count;
    }

}
