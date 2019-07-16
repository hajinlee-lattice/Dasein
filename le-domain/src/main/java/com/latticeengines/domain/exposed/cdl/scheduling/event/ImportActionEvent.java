package com.latticeengines.domain.exposed.cdl.scheduling.event;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.scheduling.SimulationContext;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;

public class ImportActionEvent extends Event {

    public ImportActionEvent(String tenantId, Long time) {
        super(time);
        this.tenantId = tenantId;
    }

    @Override
    public List<Event> changeState(SimulationContext simulationContext) {
        TenantActivity tenantActivity = simulationContext.getcanRunTenantActivityByTenantId(tenantId);
        if (tenantActivity != null) {
            if (tenantActivity.getFirstActionTime() == null || tenantActivity.getFirstActionTime() == 0L) {
                tenantActivity.setFirstActionTime(getTime());
            }
            if (tenantActivity.getLastActionTime() == null
                    || tenantActivity.getLastActionTime() - getTime() < 0) {
                tenantActivity.setLastActionTime(getTime());
            }
            simulationContext.setTenantActivityToCanRun(tenantActivity);
        }
        simulationContext.push(tenantId, this);
        return null;
    }

    @Override
    public String toString() {
        return "ImportActionEvent current time is: " + getTime();
    }
}
