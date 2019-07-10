package com.latticeengines.domain.exposed.cdl.scheduling.event;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.scheduling.SimulationStats;
import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;

public abstract class Event implements Comparable<Event> {
    private Long time;

    public Event(Long time) {
        this.time = time;
    }

    // change current state & tenant activities and other things etc.
    // return newly generated events
    public abstract List<Event> changeState(SystemStatus status, SimulationStats simulationStats);

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    @Override
    public int compareTo(Event e) {
        return e.getTime() - time > 0 ? -1 : 1;
    }
}
