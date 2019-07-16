package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.scheduling.event.DataCloudRefreshEvent;
import com.latticeengines.domain.exposed.cdl.scheduling.event.Event;
import com.latticeengines.domain.exposed.cdl.scheduling.event.ImportActionEvent;
import com.latticeengines.domain.exposed.cdl.scheduling.event.PAEndEvent;
import com.latticeengines.domain.exposed.cdl.scheduling.event.ScheduleNowEvent;

public class SimulationStats {

    private static final Logger log = LoggerFactory.getLogger(SimulationStats.class);

    public Map<String, TenantActivity> canRunTenantActivityMap;
    public Map<String, TenantActivity> runningTenantActivityMap;
    public List<String> tenantList;
    public Set<String> dcRefreshTenants;
    public Map<String, List<Event>> tenantEventMap = new HashMap<>();

    public TimeClock timeClock;
    public List<SchedulingPAQueue> schedulingPAQueues;

    public SimulationStats(List<String> tenantList, Set<String> dcRefreshTenants,
            Map<String, TenantActivity> canRunTenantActivityMap) {
        this.tenantList = tenantList;
        this.dcRefreshTenants = dcRefreshTenants;
        this.canRunTenantActivityMap = canRunTenantActivityMap;
        this.runningTenantActivityMap = new HashMap<>();
    }

    public void setSchedulingPAQueues(List<SchedulingPAQueue> schedulingPAQueues) {
        this.schedulingPAQueues = schedulingPAQueues;
    }

    public void setTimeClock(TimeClock timeClock) {
        this.timeClock = timeClock;
    }

    public List<TenantActivity> getCanRunTenantActivity() {
        return new ArrayList<>(canRunTenantActivityMap.values());
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
        if (tenantActivity.getFirstActionTime() != null && tenantActivity.getFirstActionTime() != 0L) {
            tenantActivity.setFirstActionTime(0L);
        }
        if (tenantActivity.getLastActionTime() != null && tenantActivity.getLastActionTime() != 0L) {
            tenantActivity.setLastActionTime(0L);
        }
        if (tenantActivity.isRetry()) {
            tenantActivity.setRetry(false);
        }
        return tenantActivity;
    }

    public TenantActivity setTenantActivityAfterPAFinished(TenantActivity tenantActivity) {
        // TODO probably remove this method
        if (tenantEventMap.containsKey(tenantActivity.getTenantId())) {
            List<Event> events = tenantEventMap.get(tenantActivity.getTenantId());
            for (int i = events.size() - 1; i >= 0; i--) {
                if (events.get(i).getClass().equals(PAEndEvent.class)) {
                    break;
                }
                // TODO move to import action event
                if (events.get(i).getClass().equals(ImportActionEvent.class)) {
                    tenantActivity.setLastActionTime(events.get(i).getTime());
                    if (tenantActivity.getFirstActionTime() == null || tenantActivity.getFirstActionTime() == 0L
                            || tenantActivity.getFirstActionTime() > events.get(i).getTime()) {
                        tenantActivity.setFirstActionTime(events.get(i).getTime());
                    }
                }
            }
        }
        return tenantActivity;
    }

    public void printSummary() {
        log.info(JsonUtils.serialize(tenantEventMap));
    }

    public void printMyself() {
        log.info(JsonUtils.serialize(this));
    }

    /**
     * according the pushed event, edit the tenantActivity states waiting for next
     * scheduling
     */
    public void push(String tenantId, Event e) {
        List<Event> events;
        if (!tenantEventMap.containsKey(tenantId)) {
            events = new ArrayList<>();
        } else {
            events = tenantEventMap.get(tenantId);
        }
        events.add(e);
        if (canRunTenantActivityMap.containsKey(tenantId)) {
            // TODO move to each event
            TenantActivity tenantActivity = canRunTenantActivityMap.get(tenantId);
            if (e.getClass().equals(ImportActionEvent.class)) {
                if (tenantActivity.getFirstActionTime() == null || tenantActivity.getFirstActionTime() == 0L) {
                    tenantActivity.setFirstActionTime(e.getTime());
                }
                if (tenantActivity.getLastActionTime() == null
                        || tenantActivity.getLastActionTime() - e.getTime() < 0) {
                    tenantActivity.setLastActionTime(e.getTime());
                }
            } else if (e.getClass().equals(ScheduleNowEvent.class)) {
                tenantActivity.setScheduledNow(true);
                tenantActivity.setScheduleTime(this.timeClock.getCurrentTime());
            } else if (e.getClass().equals(DataCloudRefreshEvent.class)) {
                tenantActivity.setDataCloudRefresh(true);
            }
            log.info("tenant: " + tenantId + " , tenantActivity is : " + JsonUtils.serialize(tenantActivity));
            canRunTenantActivityMap.put(tenantId, tenantActivity);
        }
        tenantEventMap.put(tenantId, events);
    }

    /**
     * if this PA job is large job, the failure probability is 2/5 if not, the
     * failure probability is 1/10
     */
    public boolean isSucceed(TenantActivity tenantActivity) {
        Random r = new Random();
        int num = r.nextInt(100);
        if (tenantActivity.isLarge()) {
            return num >= 40;
        } else {
            return num >= 10;
        }
    }

}
