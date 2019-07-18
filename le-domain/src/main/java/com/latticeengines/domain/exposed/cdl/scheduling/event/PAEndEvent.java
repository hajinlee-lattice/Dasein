package com.latticeengines.domain.exposed.cdl.scheduling.event;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.scheduling.SimulationContext;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;

public class PAEndEvent extends Event {

    public PAEndEvent(String tenantId, Long time) {
        super(time);
        this.tenantId = tenantId;
    }

    @Override
    public List<Event> changeState(SimulationContext simulationContext) {
        TenantActivity tenantActivity = simulationContext.getRuningTenantActivityByTenantId(tenantId);
        // TODO remove check (fail if not exist)
        if (tenantActivity != null) {
            // TODO move to central place for running counts
            simulationContext.systemStatus.changeSystemStateAfterPAFinished(tenantActivity);
            boolean isSuccessed = simulationContext.isSucceed(tenantId);
            if (isSuccessed || tenantActivity.isRetry()) {
                tenantActivity = simulationContext.cleanTenantActivity(tenantActivity);
                if (tenantActivity.isAutoSchedule()
                        && tenantActivity.getInvokeTime() < simulationContext.timeClock.getCurrentTime()) {
                    while (tenantActivity.getInvokeTime() - simulationContext.timeClock.getCurrentTime() < 0) {
                        long time = tenantActivity.getInvokeTime() + 24 * 3600 * 1000;
                        tenantActivity.setInvokeTime(time);
                    }
                }
            } else {
                tenantActivity.setRetry(true);
                tenantActivity.setLastFinishTime(simulationContext.timeClock.getCurrentTime());
                simulationContext.setTenantSummary(tenantActivity, true);
            }
            if (simulationContext.tenantEventMap.containsKey(tenantActivity.getTenantId())) {
                List<Event> events = simulationContext.tenantEventMap.get(tenantActivity.getTenantId());
                for (int i = events.size() - 1; i >= 0; i--) {
                    if (events.get(i) instanceof PAEndEvent) {
                        break;
                    }
                    if (events.get(i) instanceof ImportActionEvent) {
                        tenantActivity.setLastActionTime(events.get(i).getTime());
                        if (tenantActivity.getFirstActionTime() == null || tenantActivity.getFirstActionTime() == 0L
                                || tenantActivity.getFirstActionTime() > events.get(i).getTime()) {
                            tenantActivity.setFirstActionTime(events.get(i).getTime());
                        }
                    }
                }
            }
            simulationContext.changeSimulationStateAfterPAFinished(tenantActivity);
        }
        simulationContext.push(tenantId, this);
        return null;
    }

    @Override
    public String toString() {
        return "PA end for tenant: " + tenantId + ", time: " +  + getTime();
    }
}
