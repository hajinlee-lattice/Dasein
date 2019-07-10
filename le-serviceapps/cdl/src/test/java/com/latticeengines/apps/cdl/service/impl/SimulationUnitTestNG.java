package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

import javax.inject.Inject;

import org.testng.annotations.BeforeClass;

import com.latticeengines.apps.cdl.service.SchedulingPAService;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingPATestTimeClock;
import com.latticeengines.domain.exposed.cdl.scheduling.SimulationStats;
import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.event.Event;
import com.latticeengines.domain.exposed.cdl.scheduling.event.ImportActionEvent;
import com.latticeengines.domain.exposed.cdl.scheduling.event.ScheduleNowEvent;
import com.latticeengines.domain.exposed.cdl.scheduling.event.SchedulingEvent;

public class SimulationUnitTestNG {

    @Inject
    public SchedulingPAService schedulingPAService;
    public SimulationStats simulationStats;
    public PriorityQueue<Event> priorityQueue = new PriorityQueue<>();
    SchedulingPATestTimeClock clock;

    @BeforeClass(groups = "unit")
    public void setup() {
        clock = new SchedulingPATestTimeClock("2018-10-29 15:31:43");
        SystemStatus systemStatus = newStatus(5, 10, 2);
        simulationStats = new SimulationStats(clock);
        priorityQueue.addAll(generateTenantEvents());
        priorityQueue.addAll(generateSchedulingEvent());
    }

    public void testMain() {
        while (!priorityQueue.isEmpty()) {
            Event e = priorityQueue.poll();

            // fake current time
//            clock = new SchedulingPATestTimeClock(e.getTime());

//            priorityQueue.addAll(e.changeState(status));
        }
    }

    private List<Event> generateTenantEvents() {
        return Arrays.asList(new ImportActionEvent("tenant1", getRandomTime()), new ScheduleNowEvent("tenant2", getRandomTime()));
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

    private long getRandomTime() {
        Random r = new Random();
        long time = clock.getCurrentTime() + r.nextInt(100) * 3600000;
        return time;
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
}
