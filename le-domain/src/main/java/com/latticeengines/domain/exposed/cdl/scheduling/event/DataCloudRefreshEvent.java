package com.latticeengines.domain.exposed.cdl.scheduling.event;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.domain.exposed.cdl.scheduling.SimulationStats;
import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;

public class DataCloudRefreshEvent extends Event {

    public DataCloudRefreshEvent(String tenantId, Long time) {
        super(time);
        this.tenantId = tenantId;
    }

    @Override
    public List<Event> changeState(SystemStatus status, SimulationStats simulationStats) {
        simulationStats.push(tenantId, this);
        long time = 6 * 7 * 24 * 3600 + simulationStats.timeClock.getCurrentTime();
        List<Event> events = new ArrayList<>();
        DataCloudRefreshEvent dataCloudRefreshEvent = new DataCloudRefreshEvent(tenantId, time);
        events.add(dataCloudRefreshEvent);
        return events;
    }
}
