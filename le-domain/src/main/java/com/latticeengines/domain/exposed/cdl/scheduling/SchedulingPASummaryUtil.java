package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.scheduling.event.Event;

public class SchedulingPASummaryUtil {

    private static final Logger log = LoggerFactory.getLogger(SchedulingPASummaryUtil.class);

    public static String printTenantSummary(List<SimulationTenantSummary> simulationTenantSummaryList, Map<String,
            List<Event>> tenantEventMap,
                                            int schedulingEventCount, int dataCloudRefresh) {
        StringBuilder str = new StringBuilder(" ");
        str.append("this simulation summary: schedulingEventCount: ").append(schedulingEventCount).append(", " +
                "dataCloudRefreshCount: ").append(dataCloudRefresh).append(";").append("\n");
        for (SimulationTenantSummary simulationTenantSummary : simulationTenantSummaryList) {
            List<Event> events = tenantEventMap.get(simulationTenantSummary.getTenantId());
            if (!CollectionUtils.isEmpty(events)) {
                str.append(simulationTenantSummary.getTenantSummary(events));
                str.append("\n");
            }
        }
        return str.toString();
    }
}
