package com.latticeengines.apps.cdl.service.impl;

import java.util.HashSet;
import java.util.List;

import javax.inject.Inject;

import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.SchedulingPAService;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingPAQueue;
import com.latticeengines.domain.exposed.cdl.scheduling.Simulation;
import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;

public class SimulationUnitTestNG {

    @Inject
    private SchedulingPAService schedulingPAService;

    @Test(groups = "unit")
    public void testMain() {
        SystemStatus systemStatus = newStatus(5, 10, 2);
        Simulation simulation = new Simulation(systemStatus);
        List<TenantActivity> tenantActivityList = simulation.getActivityList();
        List<SchedulingPAQueue> schedulingPAQueues = schedulingPAService.initQueue(simulation.getTimeClock(),
                systemStatus, tenantActivityList);
        simulation.setSchedulingPAQueues(schedulingPAQueues);
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

}
