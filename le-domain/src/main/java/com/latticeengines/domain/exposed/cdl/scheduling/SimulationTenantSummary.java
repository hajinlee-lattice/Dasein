package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.LinkedList;
import java.util.List;

import com.latticeengines.domain.exposed.cdl.scheduling.event.Event;
import com.latticeengines.domain.exposed.cdl.scheduling.event.ImportActionEvent;
import com.latticeengines.domain.exposed.cdl.scheduling.event.PAEndEvent;
import com.latticeengines.domain.exposed.cdl.scheduling.event.PAStartEvent;
import com.latticeengines.domain.exposed.cdl.scheduling.event.ScheduleNowEvent;
import com.latticeengines.domain.exposed.security.TenantType;

public class SimulationTenantSummary {

    private String tenantId;
    private boolean isLarge;
    private boolean isDataCloudRefresh;
    private TenantType tenantType;
    private int paNum;
    private int failedPANum;
    private int retryPANum;
    private float successRate;
    private List<Long> paTime;
    private List<Long> importActionWaitingTime;
    private List<Long> scheduleNowWaitingTime;
    private Long maxPATime;
    private Long minPATime;
    private Long maxImportActionWaitingTime;
    private Long minImportActionWaitingTime;
    private Long maxScheduleNowWaitingTime;
    private Long minScheduleNowWaitingTime;

    public SimulationTenantSummary(String tenantId, boolean isLarge, boolean isDataCloudRefresh, TenantType tenantType) {
        this.tenantId = tenantId;
        this.isLarge = isLarge;
        this.isDataCloudRefresh = isDataCloudRefresh;
        this.tenantType = tenantType;
        this.paTime = new LinkedList<>();
        this.importActionWaitingTime = new LinkedList<>();
        this.scheduleNowWaitingTime = new LinkedList<>();
    }

    public void calculate(List<Event> eventList) {
        long paStartTime = 0L;
        long paEndTime = 0L;
        for (int i = eventList.size() - 1; i >= 0; i--) {
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
            maxPATime = minPATime = paTime.get(0);
            for (long patime : paTime) {
                if (patime < minPATime) {
                    minPATime = patime;
                } else if (patime > maxPATime) {
                    maxPATime = patime;
                }
                avgPATime += patime;
            }
            avgPATime = avgPATime / paTime.size();
        }
        return avgPATime;
    }

    public long getAvgImportActionWaitingTime() {
        long avgTime = 0L;
        if (importActionWaitingTime.size() > 0) {
            maxImportActionWaitingTime = minImportActionWaitingTime = importActionWaitingTime.get(0);
            for (long time : importActionWaitingTime) {
                if (time < minImportActionWaitingTime) {
                    minImportActionWaitingTime = time;
                } else if (time > maxImportActionWaitingTime){
                    maxImportActionWaitingTime = time;
                }
                avgTime += time;
            }
            avgTime = avgTime / importActionWaitingTime.size();
        }
        return avgTime;
    }

    public long getAvgScheduleNowWaitingTime() {
        long avgTime = 0L;
        if (scheduleNowWaitingTime.size() > 0) {
            minScheduleNowWaitingTime = maxScheduleNowWaitingTime = scheduleNowWaitingTime.get(0);
            for (long time : scheduleNowWaitingTime) {
                if ( time < minScheduleNowWaitingTime) {
                    minScheduleNowWaitingTime = time;
                } else if (time > maxScheduleNowWaitingTime) {
                    maxScheduleNowWaitingTime = time;
                }
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
                "successRate: %f, avgPATime: %d, MaxPATime: %d, MinPATime: %d, avgImportActionWaitingTime: %d, " +
                        "MaxImportActionWaitingTime: %d, MinImportActionWaitingTime: %d, " +
                        "avgScheduleNowWaitingTime: %d, MaxScheduleNowWaitingTime: %d, MinScheduleNowWaitingTime: %d.",
                tenantId, isLarge, isDataCloudRefresh, paNum, failedPANum, retryPANum, getSuccessRate(),
                getAvgPATime(), maxPATime, minPATime, getAvgImportActionWaitingTime(),
                maxImportActionWaitingTime, minImportActionWaitingTime, getAvgScheduleNowWaitingTime(),
                maxScheduleNowWaitingTime, minScheduleNowWaitingTime);
        return result;
    }

    public TenantType getTenantType() {
        return tenantType;
    }

    public List<Long> getPaTime() {
        return this.paTime;
    }

    public List<Long> getImportActionWaitingTime() {
        return this.importActionWaitingTime;
    }

    public List<Long> getScheduleNowWaitingTime() {
        return this.scheduleNowWaitingTime;
    }

    public Long getMaxPATime() {
        return this.maxPATime;
    }

    public Long getMinPATime() {
        return this.minPATime;
    }

    public Long getMaxImportActionWaitingTime() {
        return this.maxImportActionWaitingTime;
    }

    public Long getMinImportActionWaitingTime() {
        return this.minImportActionWaitingTime;
    }

    public Long getMaxScheduleNowWaitingTime() {
        return this.maxScheduleNowWaitingTime;
    }

    public Long getMinScheduleNowWaitingTime() {
        return this.minScheduleNowWaitingTime;
    }
}
