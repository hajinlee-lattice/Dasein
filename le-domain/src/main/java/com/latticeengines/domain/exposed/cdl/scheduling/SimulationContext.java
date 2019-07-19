package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.cdl.scheduling.event.Event;

public class SimulationContext {

    private static final Logger log = LoggerFactory.getLogger(SimulationContext.class);

    private Map<String, TenantActivity> canRunTenantActivityMap;
    private Map<String, TenantActivity> runningTenantActivityMap;
    private Map<String, SimulationTenant> simulationTenantMap;
    private Map<String, SimulationTenantSummary> simulationTenantSummaryMap;
    public final Set<String> dcRefreshTenants;
    public SystemStatus systemStatus;
    public Map<String, List<Event>> tenantEventMap = new HashMap<>();

    public TimeClock timeClock;

    private int schedulingEventCount;
    private int dataCloudRefreshCount;


    public SimulationContext(SystemStatus systemStatus, Set<String> dcRefreshTenants,
                             Map<String, SimulationTenant> simulationTenantMap) {
        this.systemStatus = systemStatus;
        this.dcRefreshTenants = dcRefreshTenants;
        this.simulationTenantMap = simulationTenantMap;
        setCanRunTenantActivityMap();
        this.runningTenantActivityMap = new HashMap<>();
    }

    private void setCanRunTenantActivityMap() {
        Iterator iter = this.simulationTenantMap.entrySet().iterator();
        this.canRunTenantActivityMap = new HashMap<>();
        this.simulationTenantSummaryMap = new HashMap<>();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            String tenantId = entry.getKey().toString();
            SimulationTenant simulationTenant = (SimulationTenant) entry.getValue();
            TenantActivity tenantActivity = simulationTenant.getTenantActivity();
            SimulationTenantSummary simulationTenantSummary = new SimulationTenantSummary(tenantId,
                    tenantActivity.isLarge(), dcRefreshTenants.contains(tenantId));
            this.canRunTenantActivityMap.put(tenantId, tenantActivity);
            this.simulationTenantSummaryMap.put(tenantId, simulationTenantSummary);
        }
    }

    public int getRandomTime(String tenantId) {
        if (simulationTenantMap.containsKey(tenantId)) {
            SimulationTenant simulationTenant = simulationTenantMap.get(tenantId);
            return simulationTenant.getRandom();
        }
        return 0;
    }

    public boolean isSucceed(String tenantId) {
        if (simulationTenantMap.containsKey(tenantId)) {
            SimulationTenant simulationTenant = simulationTenantMap.get(tenantId);
            return simulationTenant.isSucceed();
        }
        return false;
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

    public void printSummary() {
        log.info(SchedulingPASummaryUtil.printTenantSummary(this));

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
        tenantEventMap.put(tenantId, events);
    }

    public void setTenantActivityToCanRun(TenantActivity tenantActivity) {
        if (canRunTenantActivityMap.containsKey(tenantActivity.getTenantId())) {
            canRunTenantActivityMap.put(tenantActivity.getTenantId(), tenantActivity);
        }
    }

    public List<TenantActivity> getRuningTenantActivitys() {
        return new ArrayList<>(runningTenantActivityMap.values());
    }

    public void setFailedCount(String tenantId, boolean isFailed) {
        if (simulationTenantSummaryMap.containsKey(tenantId)) {
            if (isFailed) {
                SimulationTenantSummary simulationTenantSummary =
                        simulationTenantSummaryMap.get(tenantId);
                simulationTenantSummary.setFailedPANum(simulationTenantSummary.getFailedPANum() + 1);
            }
        }
    }

    public void setRetryCount(String tenantId, boolean isRetry) {
        if (simulationTenantSummaryMap.containsKey(tenantId)) {
            if (isRetry) {
                SimulationTenantSummary simulationTenantSummary =
                        simulationTenantSummaryMap.get(tenantId);
                //when pa running, Retry flag is true ,then this is retry PA
                simulationTenantSummary.setRetryPANum(simulationTenantSummary.getRetryPANum() + 1);
            }
        }
    }

    public int getSchedulingEventCount() {
        return schedulingEventCount;
    }

    public void setSchedulingEventCount(int schedulingEventCount) {
        this.schedulingEventCount = schedulingEventCount;
    }

    public int getDataCloudRefreshCount() {
        return dataCloudRefreshCount;
    }

    public void setDataCloudRefreshCount(int dataCloudRefreshCount) {
        this.dataCloudRefreshCount = dataCloudRefreshCount;
    }

    public Map<String, SimulationTenantSummary> getSimulationTenantSummaryMap() {
        return this.simulationTenantSummaryMap;
    }
}
