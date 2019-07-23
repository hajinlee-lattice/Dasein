package com.latticeengines.domain.exposed.cdl.scheduling;

public class SimulationTimeClock implements TimeClock {

    private long timestamp;

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public long getCurrentTime() {
        return timestamp;
    }
}
