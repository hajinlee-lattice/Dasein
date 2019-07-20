package com.latticeengines.domain.exposed.cdl.scheduling.event;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.scheduling.SimulationContext;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;

public class ScheduleNowEvent extends Event {

    public ScheduleNowEvent(String tenantId, Long time) {
        super(time);
        this.tenantId = tenantId;
    }

    @Override
    public List<Event> changeState(SimulationContext simulationContext) {
        TenantActivity tenantActivity = simulationContext.getcanRunTenantActivityByTenantId(tenantId);
        if (tenantActivity != null) {
            tenantActivity.setScheduledNow(true);
            tenantActivity.setScheduleTime(simulationContext.timeClock.getCurrentTime());
            simulationContext.setTenantActivityToCanRun(tenantActivity);
        }
        simulationContext.push(tenantId, this);
        return null;
    }

    @Override
    public String toString() {
        return "ScheduleNowEvent tenantId is: " + tenantId + ", current time is: " + getTime();
    }
}
