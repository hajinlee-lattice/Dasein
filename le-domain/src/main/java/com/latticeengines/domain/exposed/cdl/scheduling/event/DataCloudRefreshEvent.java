package com.latticeengines.domain.exposed.cdl.scheduling.event;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.scheduling.SimulationStats;
import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;

public class DataCloudRefreshEvent extends Event {

    public DataCloudRefreshEvent(Long time) {
        super(time);
    }

    @Override
    public List<Event> changeState(SystemStatus status, SimulationStats simulationStats) {
        for (String tenantId : simulationStats.dcRefreshTenants) {
            TenantActivity activity = simulationStats.getcanRunTenantActivityByTenantId(tenantId);
            if (activity == null) {
                activity = simulationStats.getRuningTenantActivityByTenantId(tenantId);
            }
            activity.setDataCloudRefresh(true);
            simulationStats.push(tenantId, this);
        }

        return null;
    }
}
