package com.latticeengines.apps.cdl.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.scheduling.SimulationContext;
import com.latticeengines.domain.exposed.cdl.scheduling.SimulationTenant;
import com.latticeengines.domain.exposed.cdl.scheduling.SimulationTimeClock;
import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.event.DataCloudRefreshEvent;
import com.latticeengines.domain.exposed.cdl.scheduling.event.Event;
import com.latticeengines.domain.exposed.cdl.scheduling.event.SchedulingEvent;

public class Simulation {

    private static final Logger log = LoggerFactory.getLogger(Simulation.class);

    private SimulationContext simulationContext;
    private PriorityQueue<Event> priorityQueue;
    private SimulationTimeClock clock;
    private SystemStatus systemStatus;
    private long endTime;

    public Simulation(SystemStatus systemStatus, Set<String> dcRefreshTenants,
                      Map<String, SimulationTenant> simulationTenantMap, PriorityQueue<Event> priorityQueue,
                      SimulationTimeClock timeClock, String duringTime) {
        this.clock = timeClock;
        this.priorityQueue = priorityQueue;
        this.systemStatus = systemStatus;
        this.simulationContext = new SimulationContext(systemStatus, dcRefreshTenants, simulationTenantMap);
        this.simulationContext.setTimeClock(clock);
        this.endTime = transferTime(duringTime);
        this.priorityQueue.addAll(generateSchedulingEvent());
        this.priorityQueue.addAll(generateDataCloudRefreshEvent());
    }

    public void run() {
        while (!priorityQueue.isEmpty() && (clock.getCurrentTime() <= endTime)) {
            Event e = priorityQueue.poll();
            // fake current time
            log.debug(e.toString());
            clock.setTimestamp(e.getTime());
            List<Event> events = e.changeState(simulationContext);
            if (!CollectionUtils.isEmpty(events)) {
                priorityQueue.addAll(events);
            }
        }
        log.info(JsonUtils.serialize(this.systemStatus));
        simulationContext.printSummary();
    }

    private long transferTime(String duringTime) {
        String timeNumber = duringTime.substring(0, duringTime.length() - 1);
        String tag = duringTime.substring(duringTime.length() - 1);
        long endTime = 0L;
        switch (tag) {
            case "d" : endTime = 86400L * 1000 * Long.valueOf(timeNumber) + clock.getCurrentTime(); break;
            case "w" : endTime = 7 * 86400L * 1000 * Long.valueOf(timeNumber) + clock.getCurrentTime(); break;
            case "m" : endTime = 30 * 7 * 86400L * 1000 * Long.valueOf(timeNumber) + clock.getCurrentTime(); break;
            default: break;
        }
        return endTime;
    }

    private List<Event> generateDataCloudRefreshEvent() {
        List<Event> events = new ArrayList<>();
        long time = clock.getCurrentTime();
        for (int i = 0; time <= endTime ; i++) {
            // 6 wks
            time += i * 6 * 7 * 86400L * 1000;
            events.add(new DataCloudRefreshEvent(time));
        }

        return events;
    }

    private List<Event> generateSchedulingEvent() {
        long time = clock.getCurrentTime();
        List<Event> events = new ArrayList<>();
        for (int i = 0; time <= endTime; i++) {
            time += 5 * 60 * 1000; // 5 min
            events.add(new SchedulingEvent(time));
        }
        return events;
    }

}
