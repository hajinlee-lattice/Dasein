package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.List;

import org.springframework.util.CollectionUtils;

import com.latticeengines.domain.exposed.cdl.scheduling.event.Event;

public class SchedulingPASummaryUtil {

    public static String printTenantSummary(SimulationContext simulationContext) {
        StringBuilder str = new StringBuilder(" ");
        str.append("this simulation summary: schedulingEventCount: ").append(simulationContext.getSchedulingEventCount()).append(", " +
                "dataCloudRefreshCount: ").append(simulationContext.getDataCloudRefreshCount()).append(";").append("\n");
        for (SimulationTenantSummary simulationTenantSummary : simulationContext.getSimulationTenantSummaryMap().values()) {
            List<Event> events = simulationContext.tenantEventMap.get(simulationTenantSummary.getTenantId());
            if (!CollectionUtils.isEmpty(events)) {
                str.append(simulationTenantSummary.getTenantSummary(events));
                str.append("\n");
            }
        }
        return str.toString();
    }
}
