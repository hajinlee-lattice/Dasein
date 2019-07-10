package com.latticeengines.domain.exposed.cdl.scheduling.event;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.scheduling.SimulationStats;
import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;

public class ImportActionEvent extends Event {

    private String tenantId;

    public ImportActionEvent(String tenantId, Long time) {
        super(time);
        this.tenantId = tenantId;
    }

    @Override
    public List<Event> changeState(SystemStatus status, SimulationStats simulationStats) {
        simulationStats.push(tenantId, this);
        return null;
    }
}
