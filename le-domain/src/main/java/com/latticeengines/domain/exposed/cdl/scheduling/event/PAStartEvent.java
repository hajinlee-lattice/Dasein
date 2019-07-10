package com.latticeengines.domain.exposed.cdl.scheduling.event;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.scheduling.SimulationStats;
import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;

public class PAStartEvent extends Event {

    private String tenantId;

    public PAStartEvent(String tenantId, Long time) {
        super(time);
        this.tenantId = tenantId;
    }

    @Override
    public List<Event> changeState(SystemStatus status, SimulationStats simulationStats) {
        TenantActivity tenantActivity = simulationStats.getcanRunTenantActivityByTenantId(tenantId);
        status.changeSystemState(tenantActivity);
        simulationStats.changeSimulationStateWhenRunPA(tenantActivity);
        simulationStats.push(tenantId, this);
        return null;
    }
}
