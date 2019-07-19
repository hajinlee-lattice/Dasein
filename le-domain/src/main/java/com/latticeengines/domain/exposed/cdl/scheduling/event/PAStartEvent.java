package com.latticeengines.domain.exposed.cdl.scheduling.event;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.cdl.scheduling.SimulationContext;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;

public class PAStartEvent extends Event {

    private boolean isRetry;

    private static final Logger log = LoggerFactory.getLogger(PAStartEvent.class);

    public PAStartEvent(String tenantId, Long time) {
        super(time);
        this.tenantId = tenantId;
    }

    @Override
    public List<Event> changeState(SimulationContext simulationContext) {
        TenantActivity tenantActivity = simulationContext.getcanRunTenantActivityByTenantId(tenantId);
        if (tenantActivity != null) {
            this.isRetry = tenantActivity.isRetry();
            simulationContext.setRetryCount(tenantId, isRetry);
            log.info(simulationContext.systemStatus.toString());
            simulationContext.changeSimulationStateWhenRunPA(tenantActivity);
            simulationContext.push(tenantId, this);
        }
        return null;
    }

    @Override
    public String toString() {
        return "PA start for tenant: " + tenantId + ", time: " + getTime() + ", isRetry: " + isRetry;
    }
}
