package com.latticeengines.domain.exposed.cdl.scheduling.event;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.scheduling.SimulationContext;

public class VerifyEvent extends Event {

    private boolean verifyed;

    public VerifyEvent(long time) {
        super(time);
        this.tenantId = "";
    }

    @Override
    public List<Event> changeState(SimulationContext simulationContext) {
        verifyed =
                simulationContext.systemStatus.getCanRunJobCount() >= 0 && simulationContext.systemStatus.getCanRunLargeJobCount() >= 0;
        simulationContext.verifyEventList.add(this);
        return null;
    }

    public boolean isVerifyed() {
        return this.verifyed;
    }

    @Override
    public String toString() {
        return "Verify Event time: " + getTime() + ", verifyed: " + verifyed;
    }
}
