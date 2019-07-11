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
            boolean isSuccessed = simulationStats.isSucceed(tenantActivity);
            if (isSuccessed || (!isSuccessed && tenantActivity.isRetry())) {
                tenantActivity = simulationStats.cleanTenantActivity(tenantActivity);
                if (tenantActivity.isAutoSchedule() && tenantActivity.getInvokeTime() < simulationStats.timeClock.getCurrentTime()) {
                    while (tenantActivity.getInvokeTime() - simulationStats.timeClock.getCurrentTime() < 0) {
                        long time = tenantActivity.getInvokeTime() + 24 * 3600;
                        tenantActivity.setInvokeTime(time);
                    }
                }
            } else {
                tenantActivity.setRetry(true);
                tenantActivity.setLastFinishTime(simulationStats.timeClock.getCurrentTime());
            }
            tenantActivity = simulationStats.setTenantActivityAfterPAFinished(tenantActivity);
            simulationStats.changeSimulationStateAfterPAFinished(tenantActivity);
        }
        simulationStats.push(tenantId, this);
        return null;
    }
}
