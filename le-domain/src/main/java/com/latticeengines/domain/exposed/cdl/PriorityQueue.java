package com.latticeengines.domain.exposed.cdl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;

public class PriorityQueue<T extends PriorityObject> {

    private static SystemStatus systemStatus;

    private PriorityBlockingQueue<T> priorityBlockingQueue;

    public PriorityQueue() {
        priorityBlockingQueue = new PriorityBlockingQueue<>();
    }

    public List<String> getAll() {
        return getAllMemberFromQueue(priorityBlockingQueue);
    }

    public int getPosition(String tenantName) {
        return getPositionFromQueue(priorityBlockingQueue, tenantName);
    }

    public String peek() {
        return peekFromPriorityQueue(priorityBlockingQueue);
    }

    public String poll() {
        return pollFromPriorityQueue(priorityBlockingQueue);
    }

    public void add(T priorityObject) {
        priorityBlockingQueue.add(priorityObject);
    }

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


    private List<String> getAllMemberFromQueue(PriorityBlockingQueue<T> priorityBlockingQueue) {
        List<String> memberList = new ArrayList<>();
        List<T> priorityObjectList = new LinkedList<>();
        while (priorityBlockingQueue.peek() != null) {
            T priorityObject = priorityBlockingQueue.poll();
            priorityObjectList.add(priorityObject);
            memberList.add(priorityObject.getTenantId());
        }
        priorityBlockingQueue.addAll(priorityObjectList);
        return memberList;
    }

    private int getPositionFromQueue(PriorityBlockingQueue<T> priorityBlockingQueue,
                                                                String tenantId) {
        List<T> priorityObjectList = new LinkedList<>();
        int index = 1;
        while (priorityBlockingQueue.peek() != null) {
            T priorityObject = priorityBlockingQueue.poll();
            priorityObjectList.add(priorityObject);
            if (tenantId.equalsIgnoreCase(priorityObject.getTenantId())) {
                priorityBlockingQueue.addAll(priorityObjectList);
                return index;
            }
            index++;
        }
        priorityBlockingQueue.addAll(priorityObjectList);
        return -1;
    }

    private String peekFromPriorityQueue(PriorityBlockingQueue<T> priorityBlockingQueue) {
        T priorityObject = priorityBlockingQueue.peek();
        return (priorityObject != null && checkViolated(priorityObject)) ? priorityObject.getTenantId() : null;
    }

    private String pollFromPriorityQueue(PriorityBlockingQueue<T> priorityBlockingQueue) {
        T priorityObject = priorityBlockingQueue.peek();
        if (priorityObject != null && checkViolated(priorityObject)) {
            changeSystemState(priorityObject);
            priorityBlockingQueue.poll();
            return priorityObject.getTenantId();
        }
        return null;
    }

    private void changeSystemState(T priorityObject) {
        systemStatus.setCanRunJobCount(systemStatus.getCanRunJobCount() - 1);
        if (priorityObject.isLarge()) {
            systemStatus.setCanRunLargeJobCount(systemStatus.getRunningLargeJobCount() - 1);
        }
        if (priorityObject.isScheduledNow()) {
            systemStatus.setCanRunScheduleNowJobCount(systemStatus.getRunningScheduleNowCount() - 1);
        }
    }

    public void setSystemStatus(int canRunJobCount, int canRunLargeJobCount, int canRunScheduleNowJobCount,
                                int runningTotalCount, int runningScheduleNowCount, int runningLargeJobCount,
                                Set<String> largeJobTenantId, Set<String> runningPATenantId) {
        systemStatus = new SystemStatus();
        systemStatus.setCanRunJobCount(canRunJobCount);
        systemStatus.setCanRunLargeJobCount(canRunLargeJobCount);
        systemStatus.setCanRunScheduleNowJobCount(canRunScheduleNowJobCount);
        systemStatus.setRunningTotalCount(runningTotalCount);
        systemStatus.setRunningLargeJobCount(runningLargeJobCount);
        systemStatus.setRunningScheduleNowCount(runningScheduleNowCount);
        systemStatus.setLargeJobTenantId(largeJobTenantId);
        systemStatus.setRunningPATenantId(runningPATenantId);
    }

    public boolean isLargeJob(String tenantId) {
        return systemStatus.getLargeJobTenantId().contains(tenantId);
    }

    public List<String> getRunningPATenantId() {
        return new ArrayList<>(systemStatus.getRunningPATenantId());
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

    static class SystemStatus {
        private int canRunJobCount;
        private int canRunLargeJobCount;
        private int canRunScheduleNowJobCount;
        private int runningTotalCount;
        private int runningScheduleNowCount;
        private int runningLargeJobCount;
        private Set<String> largeJobTenantId;
        private  Set<String> runningPATenantId;


        public int getCanRunJobCount() {
            return canRunJobCount;
        }

        public void setCanRunJobCount(int canRunJobCount) {
            this.canRunJobCount = canRunJobCount;
        }

        public int getCanRunLargeJobCount() {
            return canRunLargeJobCount;
        }

        public void setCanRunLargeJobCount(int canRunLargeJobCount) {
            this.canRunLargeJobCount = canRunLargeJobCount;
        }

        public int getCanRunScheduleNowJobCount() {
            return canRunScheduleNowJobCount;
        }

        public void setCanRunScheduleNowJobCount(int canRunScheduleNowJobCount) {
            this.canRunScheduleNowJobCount = canRunScheduleNowJobCount;
        }

        public int getRunningTotalCount() {
            return runningTotalCount;
        }

        public void setRunningTotalCount(int runningTotalCount) {
            this.runningTotalCount = runningTotalCount;
        }

        public int getRunningScheduleNowCount() {
            return runningScheduleNowCount;
        }

        public void setRunningScheduleNowCount(int runningScheduleNowCount) {
            this.runningScheduleNowCount = runningScheduleNowCount;
        }

        public int getRunningLargeJobCount() {
            return runningLargeJobCount;
        }

        public void setRunningLargeJobCount(int runningLargeJobCount) {
            this.runningLargeJobCount = runningLargeJobCount;
        }

        public Set<String> getLargeJobTenantId() {
            return largeJobTenantId;
        }

        public void setLargeJobTenantId(Set<String> largeJobTenantId) {
            this.largeJobTenantId = largeJobTenantId;
        }

        public Set<String> getRunningPATenantId() {
            return runningPATenantId;
        }

        public void setRunningPATenantId(Set<String> runningPATenantId) {
            this.runningPATenantId = runningPATenantId;
        }
    }
}
