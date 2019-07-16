package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.scheduling.event.Event;
import com.latticeengines.domain.exposed.cdl.scheduling.event.PAEndEvent;
import com.latticeengines.domain.exposed.cdl.scheduling.event.PAStartEvent;

public class SimulationTenantSummary {

    private String tenantId;
    private Map<Long, Event> eventMap;
    private long preTime;

    public SimulationTenantSummary(String tenantId) {
        this.tenantId = tenantId;
        this.preTime = 0L;
        this.eventMap = new HashMap<>();
    }

    public void push(Event event, long currentTime) {
        if (this.eventMap.isEmpty()) {
            eventMap.put(0L, event);
        } else {
            long duringTime = currentTime - this.preTime;
            eventMap.put(duringTime, event);
        }
        this.preTime = currentTime;
    }

    public String printSummary() {
        StringBuilder str = new StringBuilder("tenant " + tenantId + " summary is: ");
        int paStartNum = 0;
        int paEndNum = 0;
        for (Map.Entry<Long, Event> longEventEntry : this.eventMap.entrySet()) {
            if (((Map.Entry) longEventEntry).getValue() instanceof PAStartEvent) {
                paStartNum++;
            }
            if (((Map.Entry) longEventEntry).getValue() instanceof PAEndEvent) {
                paEndNum++;
            }
            str.append(((Map.Entry) longEventEntry).getValue().toString()).append(", during time is: ").append(((Map.Entry) longEventEntry).getKey().toString()).append(", ");
        }
        str.append("PA start num is: ").append(paStartNum).append(", PA end num is: ").append(paEndNum);
        return str.toString();
    }
}
