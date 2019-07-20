package com.latticeengines.domain.exposed.cdl.scheduling.event;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.cdl.scheduling.GreedyScheduler;
import com.latticeengines.domain.exposed.cdl.scheduling.Scheduler;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingPAUtil;
import com.latticeengines.domain.exposed.cdl.scheduling.SimulationContext;

public class SchedulingEvent extends Event {

    private static final Logger log = LoggerFactory.getLogger(SchedulingEvent.class);

    private String RETRY_KEY = "RETRY_KEY";
    private String OTHER_KEY = "OTHER_KEY";

    public SchedulingEvent(long time) {
        super(time);
        this.tenantId = "";
    }

    @Override
    public List<Event> changeState(SimulationContext simulationContext) {
        // init scheduler
        Scheduler scheduler = new GreedyScheduler();
        // schedule PA jobs
        Map<String, Set<String>> tenantMap = scheduler.schedule(SchedulingPAUtil.initQueue(simulationContext));
        Set<String> tenantSet = new HashSet<>();
        tenantSet.addAll(tenantMap.get(RETRY_KEY));
        tenantSet.addAll(tenantMap.get(OTHER_KEY));
        List<Event> events = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(tenantSet)) {
            log.info("SchedulingEvent: " + getTime() + ", tenants=" + tenantSet + ", size=" + tenantSet.size());
            log.info("running tenant is: " + simulationContext.getRuningTenantActivitys());
            log.info("systemStatus is: " + simulationContext.systemStatus.toString());
            for (String tenantId : tenantSet) {
                PAStartEvent paStartEvent = new PAStartEvent(tenantId, simulationContext.timeClock.getCurrentTime());
                long endTime =
                        simulationContext.timeClock.getCurrentTime() + simulationContext.getRandomTime(tenantId) * 3600L * 1000;
                PAEndEvent paEndEvent = new PAEndEvent(tenantId, endTime);
                events.add(paStartEvent);
                events.add(paEndEvent);
            }
        }
        simulationContext.setSchedulingEventCount(simulationContext.getSchedulingEventCount() + 1);
        // clear already scheduled tenant set
        simulationContext.systemStatus.setScheduleTenants(new HashSet<>());
        return events;
    }

    @Override
    public String toString() {
        return "SchedulingEvent current time is " + getTime();
    }
}
