package com.latticeengines.domain.exposed.cdl.scheduling.event;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.scheduling.SimulationStats;
import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;

public class PAEndEvent extends Event {

    private String tenantId;

    public PAEndEvent(String tenantId, Long time) {
        super(time);
        this.tenantId = tenantId;
    }

    @Override
    public List<Event> changeState(SystemStatus status, SimulationStats simulationStats) {
        TenantActivity tenantActivity = simulationStats.getRuningTenantActivityByTenantId(tenantId);
        if (tenantActivity != null) {
            status.changeSystemStateAfterPAFinished(tenantActivity);
            tenantActivity = simulationStats.cleanTenantActivity(tenantActivity);
            tenantActivity = simulationStats.setTenantActivityAfterPAFinished(tenantActivity);
            simulationStats.changeSimulationStateAfterPAFinished(tenantActivity);
        }
        simulationStats.push(tenantId, this);
        return null;
    }
}
