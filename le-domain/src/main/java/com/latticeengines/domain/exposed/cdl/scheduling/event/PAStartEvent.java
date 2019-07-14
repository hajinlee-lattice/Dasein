package com.latticeengines.domain.exposed.cdl.scheduling.event;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.scheduling.SimulationStats;
import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;

public class PAStartEvent extends Event {

    private static final Logger log = LoggerFactory.getLogger(PAStartEvent.class);

    public PAStartEvent(String tenantId, Long time) {
        super(time);
        this.tenantId = tenantId;
    }

    @Override
    public List<Event> changeState(SystemStatus status, SimulationStats simulationStats) {
        TenantActivity tenantActivity = simulationStats.getcanRunTenantActivityByTenantId(tenantId);
        log.info("pa start tenantActivity is: " + JsonUtils.serialize(tenantActivity));
        if (tenantActivity != null) {
            status.changeSystemState(tenantActivity);
            simulationStats.changeSimulationStateWhenRunPA(tenantActivity);
            simulationStats.push(tenantId, this);
        }
        return null;
    }
}
