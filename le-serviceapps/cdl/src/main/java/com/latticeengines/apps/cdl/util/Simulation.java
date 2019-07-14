package com.latticeengines.apps.cdl.util;

import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import com.latticeengines.apps.cdl.service.SchedulingPAService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingPAQueue;
import com.latticeengines.domain.exposed.cdl.scheduling.SimulationStats;
import com.latticeengines.domain.exposed.cdl.scheduling.SimulationTimeClock;
import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;
import com.latticeengines.domain.exposed.cdl.scheduling.event.Event;
import com.latticeengines.domain.exposed.cdl.scheduling.event.SchedulingEvent;

public class Simulation {

    private static final Logger log = LoggerFactory.getLogger(Simulation.class);

    private SimulationStats simulationStats;
    private PriorityQueue<Event> priorityQueue;
    private SimulationTimeClock clock;
    private SystemStatus systemStatus;
    private SchedulingPAService schedulingPAService;

    public Simulation(SchedulingPAService schedulingPAService, SystemStatus systemStatus, List<String> tenantList,
                      Map<String, TenantActivity> tenantInitState
            , PriorityQueue<Event> priorityQueue, SimulationTimeClock timeClock) {
        this.schedulingPAService = schedulingPAService;
        this.clock = timeClock;
        this.priorityQueue = priorityQueue;
        this.systemStatus = systemStatus;
        this.simulationStats = new SimulationStats(tenantList, tenantInitState);
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
            log.info("current event is : "  + JsonUtils.serialize(e));
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        }
        log.info(JsonUtils.serialize(this.systemStatus));
    }

    public void setSchedulingPAQueues() {
        log.info(JsonUtils.serialize(simulationStats.getCanRunTenantActivity()));
        List<SchedulingPAQueue> schedulingPAQueues = schedulingPAService.initQueue(clock,
                systemStatus, this.simulationStats.getCanRunTenantActivity());
        this.simulationStats.setSchedulingPAQueues(schedulingPAQueues);
    }

    public TimeClock getTimeClock() {
        return this.clock;
    }


}
