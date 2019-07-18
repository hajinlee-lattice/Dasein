package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.cdl.scheduling.event.Event;
import com.latticeengines.domain.exposed.cdl.scheduling.event.ImportActionEvent;
import com.latticeengines.domain.exposed.cdl.scheduling.event.PAEndEvent;
import com.latticeengines.domain.exposed.cdl.scheduling.event.PAStartEvent;
import com.latticeengines.domain.exposed.cdl.scheduling.event.ScheduleNowEvent;

public class SimulationTenantSummary {

    private static final Logger log = LoggerFactory.getLogger(SimulationTenantSummary.class);

    private String tenantId;
    private boolean isLarge;
    private boolean isDataCloudRefresh;
    private int paNum;
    private int failedPANum;
    private int retryPANum;
    private float successRate;
    private List<Long> paTime;
    private List<Long> importActionWaitingTime;
    private List<Long> scheduleNowWaitingTime;

    public SimulationTenantSummary(String tenantId, boolean isLarge, boolean isDataCloudRefresh) {
        this.tenantId = tenantId;
        this.isLarge = isLarge;
        this.isDataCloudRefresh = isDataCloudRefresh;
        this.paNum = 0;
        this.failedPANum = 0;
        this.retryPANum = 0;
        this.successRate = 0.0f;
        this.paTime = new LinkedList<>();
        this.importActionWaitingTime = new LinkedList<>();
        this.scheduleNowWaitingTime = new LinkedList<>();

    }

    public void calculate(List<Event> eventList) {
        long paStartTime = 0L;
        long paEndTime = 0L;
        for (int i = (eventList.size() - 1); i >= 0; i--) {
            Event event = eventList.get(i);
            if (event instanceof PAStartEvent) {
                paStartTime = event.getTime();
                if (paEndTime != 0L) {
                    this.paNum = this.paNum + 1;
                    long duringPATime = paEndTime - paStartTime;
                    paTime.add(duringPATime);
                }
            } else if (event instanceof PAEndEvent) {
                paEndTime = event.getTime();
            } else if (event instanceof ImportActionEvent) {
                if (paStartTime != 0L) {
                    long waitingTime = paStartTime - event.getTime();
                    importActionWaitingTime.add(waitingTime);
                }
            } else if (event instanceof ScheduleNowEvent) {
                if (paStartTime != 0L && paStartTime - paEndTime < 0) {
                    long waitingTime = paStartTime - event.getTime();
                    scheduleNowWaitingTime.add(waitingTime);
                }
            }
        }
    }

    public int getFailedPANum() {
        return failedPANum;
    }

    public void setFailedPANum(int failedPANum) {
        this.failedPANum = failedPANum;
    }

    public int getRetryPANum() {
        return retryPANum;
    }

    public void setRetryPANum(int retryPANum) {
        this.retryPANum = retryPANum;
    }

    public long getAvgPATime() {
        long avgPATime = 0L;
        if (paTime.size() > 0) {
            for (long patime : paTime) {
                avgPATime += patime;
            }
            avgPATime = avgPATime / paTime.size();
        }
        return avgPATime;
    }

    public long getAvgImportActionWaitingTime() {
        long avgTime = 0L;
        if (importActionWaitingTime.size() > 0) {
            for (long time : importActionWaitingTime) {
                avgTime += time;
            }
            avgTime = avgTime / importActionWaitingTime.size();
        }
        return avgTime;
    }

    public long getAvgScheduleNowWaitingTime() {
        long avgTime = 0L;
        if (scheduleNowWaitingTime.size() > 0) {
            for (long time : scheduleNowWaitingTime) {
                avgTime += time;
            }
            avgTime = avgTime / scheduleNowWaitingTime.size();
        }
        return avgTime;
    }

    public float getSuccessRate() {
        if (this.paNum != 0) {
            this.successRate = 1 - (float) this.failedPANum / this.paNum;
        }
        return this.successRate;
    }

    public String getTenantId() {
        return this.tenantId;
    }

    public String getTenantSummary(List<Event> eventList) {
        calculate(eventList);
        String result = String.format("Tenant: %s, isLarge: %s, isDataCloudRefresh: %s, paNum: %d, failedPANum: %d, " +
                        "retryNum: %d, " +
                "successRate: %f, avgPATime: %d, avgImportActionWaitingTime: %d, avgScheduleNowWaitingTime: %d.",
                tenantId, isLarge, isDataCloudRefresh, paNum, failedPANum, retryPANum, getSuccessRate(),
                getAvgPATime(), getAvgImportActionWaitingTime(), getAvgScheduleNowWaitingTime());
        return result;
    }
}
