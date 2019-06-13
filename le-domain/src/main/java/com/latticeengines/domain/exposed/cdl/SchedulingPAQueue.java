package com.latticeengines.domain.exposed.cdl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

public class SchedulingPAQueue<T extends TenantActivity> {

    private SystemStatus systemStatus;

    private PriorityQueue<T> priorityQueue;

    public SchedulingPAQueue() {
        priorityQueue = new PriorityQueue<>();
    }

    public void setSystemStatus(SystemStatus systemStatus) {
        this.systemStatus = systemStatus;
    }

    public List<String> getAll() {
        return getAllMemberFromQueue(priorityQueue);
    }

    public int getPosition(String tenantName) {
        return getPositionFromQueue(priorityQueue, tenantName);
    }

    public String peek() {
        return peekFromPriorityQueue(priorityQueue);
    }

    public String poll() {
        return pollFromPriorityQueue(priorityQueue);
    }

    public void add(T priorityObject) {
        priorityQueue.add(priorityObject);
    }

    /**
     * @return a list of tenantIds to run PA
     */
    public List<String> getCanRunJobs() {
        Set<String> canRunJobList = new HashSet<>();
        String tenantId;
        do {
            tenantId = poll();
            if (tenantId == null) {
                break;
            }
            canRunJobList.add(tenantId);
        }while (true);
        return new ArrayList<>(canRunJobList);
    }


    private List<String> getAllMemberFromQueue(PriorityQueue<T> priorityQueue) {
        List<String> memberList = new ArrayList<>();
        List<T> priorityObjectList = new LinkedList<>();
        while (priorityQueue.peek() != null) {
            T priorityObject = priorityQueue.poll();
            priorityObjectList.add(priorityObject);
            memberList.add(priorityObject.getTenantId());
        }
        priorityQueue.addAll(priorityObjectList);
        return memberList;
    }

    private int getPositionFromQueue(PriorityQueue<T> priorityQueue,
                                                                String tenantId) {
        List<T> priorityObjectList = new LinkedList<>();
        int index = 1;
        while (priorityQueue.peek() != null) {
            T priorityObject = priorityQueue.poll();
            priorityObjectList.add(priorityObject);
            if (tenantId.equalsIgnoreCase(priorityObject.getTenantId())) {
                priorityQueue.addAll(priorityObjectList);
                return index;
            }
            index++;
        }
        priorityQueue.addAll(priorityObjectList);
        return -1;
    }

    private String peekFromPriorityQueue(PriorityQueue<T> priorityQueue) {
        T priorityObject = priorityQueue.peek();
        return (priorityObject != null && checkViolated(priorityObject)) ? priorityObject.getTenantId() : null;
    }

    private String pollFromPriorityQueue(PriorityQueue<T> priorityQueue) {
        T priorityObject = priorityQueue.peek();
        if (priorityObject != null && checkViolated(priorityObject)) {
            changeSystemState(priorityObject);
            priorityQueue.poll();
            return priorityObject.getTenantId();
        }
        return null;
    }

    /**
     *
     * Take tenantActivity to run PA editing the system status
     *
     */
    private void changeSystemState(T tenantActivity) {
        systemStatus.setCanRunJobCount(systemStatus.getCanRunJobCount() - 1);
        if (tenantActivity.isLarge()) {
            systemStatus.setCanRunLargeJobCount(systemStatus.getRunningLargeJobCount() - 1);
        }
        if (tenantActivity.isScheduledNow()) {
            systemStatus.setCanRunScheduleNowJobCount(systemStatus.getRunningScheduleNowCount() - 1);
        }
    }

    public Boolean checkViolated(T priorityObject) {
        if (priorityObject == null) {
            return false;
        }
        if (systemStatus.getCanRunJobCount() < 1) {
            return false;
        }
        if (systemStatus.getCanRunLargeJobCount() < 1 && priorityObject.isLarge()) {
            return false;
        }
        return systemStatus.getCanRunScheduleNowJobCount() >= 1 || !priorityObject.isScheduledNow();
    }
}
