package com.latticeengines.apps.cdl.util;

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
import com.latticeengines.domain.exposed.cdl.scheduling.event.Event;

public class Simulation {

    private static final Logger log = LoggerFactory.getLogger(Simulation.class);

    private SimulationContext simulationContext;
    private PriorityQueue<Event> priorityQueue;
    private SimulationTimeClock clock;
    private SystemStatus systemStatus;

    public Simulation(SystemStatus systemStatus, Set<String> dcRefreshTenants,
                      Map<String, SimulationTenant> simulationTenantMap, PriorityQueue<Event> priorityQueue,
                      SimulationTimeClock timeClock) {
        this.clock = timeClock;
        this.priorityQueue = priorityQueue;
        this.systemStatus = systemStatus;
        this.simulationContext = new SimulationContext(systemStatus, dcRefreshTenants, simulationTenantMap);
        this.simulationContext.setTimeClock(clock);

    }

    public void run() {
        while (!priorityQueue.isEmpty()) {
            Event e = priorityQueue.poll();
            // fake current time
            clock.setTimestamp(e.getTime());
            List<Event> events = e.changeState(simulationContext);
            if (!CollectionUtils.isEmpty(events)) {
                priorityQueue.addAll(events);
            }
            log.info("current event is : " + JsonUtils.serialize(e));
        }
        log.info(JsonUtils.serialize(this.systemStatus));
        simulationContext.printSummary();
    }

}
