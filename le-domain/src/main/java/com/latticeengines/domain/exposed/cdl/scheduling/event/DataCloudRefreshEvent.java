package com.latticeengines.domain.exposed.cdl.scheduling.event;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.scheduling.SimulationContext;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;

public class DataCloudRefreshEvent extends Event {

    public DataCloudRefreshEvent(Long time) {
        super(time);
    }

    @Override
    public List<Event> changeState(SimulationContext simulationContext) {
        for (String tenantId : simulationContext.dcRefreshTenants) {
            TenantActivity activity = simulationContext.getcanRunTenantActivityByTenantId(tenantId);
            if (activity != null) {
                activity.setDataCloudRefresh(true);
                simulationContext.setTenantActivityToCanRun(activity);
            }
            simulationContext.push(tenantId, this);
        }

        return null;
    }

    @Override
    public String toString() {
        return "DataCloudRefreshEvent current time is: " + getTime();
    }
}
