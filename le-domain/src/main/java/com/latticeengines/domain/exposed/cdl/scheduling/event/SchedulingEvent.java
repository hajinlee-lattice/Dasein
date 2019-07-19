package com.latticeengines.domain.exposed.cdl.scheduling.event;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.cdl.scheduling.GreedyScheduler;
import com.latticeengines.domain.exposed.cdl.scheduling.Scheduler;
import com.latticeengines.domain.exposed.cdl.scheduling.SimulationStats;
import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;

public class SchedulingEvent extends Event {

    private String RETRY_KEY = "RETRY_KEY";
    private String OTHER_KEY = "OTHER_KEY";

    public SchedulingEvent(long time) {
        super(time);
        this.tenantId = "";
    }

    @Override
    public List<Event> changeState(SystemStatus status, SimulationStats simulationStats) {
        // init scheduler
        Scheduler scheduler = new GreedyScheduler();
        // schedule PA jobs
        Map<String, Set<String>> tenantMap = scheduler.schedule(simulationStats.schedulingPAQueues);
        Set<String> tenantSet = new HashSet<>();
        tenantSet.addAll(tenantMap.get(RETRY_KEY));
        tenantSet.addAll(tenantMap.get(OTHER_KEY));
        System.out.println("SchedulingEvent: " + getTime() + ", tenants=" + tenantSet + ", size=" + tenantSet.size());
        List<Event> events = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(tenantSet)) {
            for (String tenantId : tenantSet) {
                PAStartEvent paStartEvent = new PAStartEvent(tenantId, simulationStats.timeClock.getCurrentTime());
                // TODO random end time, and make it configurable (30min now)
                long endTime = simulationStats.timeClock.getCurrentTime() + 1800L * 1000;
                PAEndEvent paEndEvent = new PAEndEvent(tenantId, endTime);
                // FIXME remove comment (mark change)
                events.add(paStartEvent);
                events.add(paEndEvent);
            }
        }
        // clear already scheduled tenant set
        status.setScheduleTenants(new HashSet<>());
        return events;
    }
}
