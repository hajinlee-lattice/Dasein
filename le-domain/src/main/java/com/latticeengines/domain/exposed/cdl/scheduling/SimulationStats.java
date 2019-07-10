package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.scheduling.event.Event;
import com.latticeengines.domain.exposed.cdl.scheduling.event.ImportActionEvent;
import com.latticeengines.domain.exposed.cdl.scheduling.event.PAEndEvent;
import com.latticeengines.domain.exposed.cdl.scheduling.event.ScheduleNowEvent;
import com.latticeengines.domain.exposed.security.TenantType;

public class SimulationStats {

    public static Map<String, TenantActivity> canRunTenantActivityMap;
    public static Map<String, TenantActivity> runningTenantActivityMap;
    public static List<String> tenantList;
    public static Map<String, List<Event>> tenantEventMap = new HashMap<>();

    public SchedulingPATestTimeClock schedulingPATestTimeClock;
    public List<SchedulingPAQueue> schedulingPAQueues;

    static {
        initTenant();
        setTenantInitState();
    }

    public SimulationStats(SchedulingPATestTimeClock schedulingPATestTimeClock) {
        this.schedulingPATestTimeClock = schedulingPATestTimeClock;
    }

    public void setSchedulingPAQueues(List<SchedulingPAQueue> schedulingPAQueues) {
        this.schedulingPAQueues = schedulingPAQueues;
    }

    public void setTimeClock(SchedulingPATestTimeClock schedulingPATestTimeClock) {
        this.schedulingPATestTimeClock = schedulingPATestTimeClock;
    }

    private static void initTenant() {
        tenantList = new LinkedList<>();
        for (int i = 1; i < 21; i ++) {
            tenantList.add("testTenant" + i);
        }
    }

    private static void setTenantInitState() {
        canRunTenantActivityMap = new HashMap<>();
        runningTenantActivityMap = new HashMap<>();
        int index = 0;
        for (String tenant : tenantList) {
            TenantActivity tenantActivity = new TenantActivity();
            tenantActivity.setTenantId(tenant);
            if (index % 2 == 0) {
                tenantActivity.setTenantType(TenantType.CUSTOMER);
            } else {
                tenantActivity.setTenantType(TenantType.QA);
            }
            canRunTenantActivityMap.put(tenant, tenantActivity);
            index++;
        }
    }

    public List<TenantActivity> getCanRunTenantActivity() {
        return (List<TenantActivity>) canRunTenantActivityMap.values();
    }

    public TenantActivity getcanRunTenantActivityByTenantId(String tenantId) {
        return canRunTenantActivityMap.get(tenantId);
    }

    public TenantActivity getRuningTenantActivityByTenantId(String tenantId) {
        return runningTenantActivityMap.get(tenantId);
    }

    public void changeSimulationStateWhenRunPA(TenantActivity tenantActivity) {
        if (canRunTenantActivityMap.containsKey(tenantActivity.getTenantId())) {
            runningTenantActivityMap.put(tenantActivity.getTenantId(), tenantActivity);
            canRunTenantActivityMap.remove(tenantActivity.getTenantId());
        }
    }

    public void changeSimulationStateAfterPAFinished(TenantActivity tenantActivity) {
        if (runningTenantActivityMap.containsKey(tenantActivity.getTenantId())) {
            canRunTenantActivityMap.put(tenantActivity.getTenantId(), tenantActivity);
            runningTenantActivityMap.remove(tenantActivity.getTenantId());
        }
    }

    public TenantActivity cleanTenantActivity(TenantActivity tenantActivity) {
        if (tenantActivity.isScheduledNow()) {
            tenantActivity.setScheduledNow(false);
        }
        if (tenantActivity.getFirstActionTime() != 0L) {
            tenantActivity.setFirstActionTime(0L);
        }
        if (tenantActivity.getLastActionTime() != 0L) {
            tenantActivity.setLastActionTime(0L);
        }
        if (tenantActivity.isRetry()) {
            tenantActivity.setRetry(false);
        }
        return tenantActivity;
    }

    public TenantActivity setTenantActivityAfterPAFinished(TenantActivity tenantActivity) {
        if (tenantEventMap.containsKey(tenantActivity.getTenantId())) {
            List<Event> events = tenantEventMap.get(tenantActivity.getTenantId());
            for ( int i = events.size() - 1; i >= 0; i--) {
                if (events.get(i).getClass().equals(PAEndEvent.class)) {
                    break;
                }
                if (events.get(i).getClass().equals(ImportActionEvent.class)) {
                    tenantActivity.setLastActionTime(events.get(i).getTime());
                    if (tenantActivity.getFirstActionTime() == 0L || tenantActivity.getFirstActionTime() > events.get(i).getTime()) {
                        tenantActivity.setFirstActionTime(events.get(i).getTime());
                    }
                }
            }
        }
        return tenantActivity;
    }

    public void push(String tenantId, Event e) {
        List<Event> events;
        if (!tenantEventMap.containsKey(tenantId)) {
            events = new ArrayList<>();
        } else {
            events = tenantEventMap.get(tenantId);
        }
        events.add(e);
        if (canRunTenantActivityMap.containsKey(tenantId)) {
            TenantActivity tenantActivity = canRunTenantActivityMap.get(tenantId);
            if (e.getClass().equals(ImportActionEvent.class)) {
                if (tenantActivity.getFirstActionTime() == 0L) {
                    tenantActivity.setFirstActionTime(e.getTime());
                }
                if (tenantActivity.getLastActionTime() - e.getTime() < 0) {
                    tenantActivity.setLastActionTime(e.getTime());
                }
            } else if (e.getClass().equals(ScheduleNowEvent.class)) {
                tenantActivity.setScheduledNow(true);
            }
            canRunTenantActivityMap.put(tenantId, tenantActivity);
        }
        tenantEventMap.put(tenantId, events);
    }

}
