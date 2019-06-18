package com.latticeengines.domain.exposed.cdl;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

public class SchedulingPAQueue<T extends SchedulingPAObject> {

    private SystemStatus systemStatus;

    private PriorityQueue<T> priorityQueue;

    private String objectClassName;

    public SchedulingPAQueue(SystemStatus systemStatus) {
        this.systemStatus = systemStatus;
        priorityQueue = new PriorityQueue<>();
    }

    public void setSystemStatus(SystemStatus systemStatus) {
        this.systemStatus = systemStatus;
    }

    public List<String> getAll() {
        return getAllMemberFromQueue(priorityQueue);
    }

    public String getObjectClassName() {
        return objectClassName;
    }

    public int getPosition(String tenantName) {
        return getPositionFromQueue(priorityQueue, tenantName);
    }

    public String peek(Set<String> canRunJobSet) {
        return peekFromPriorityQueue(priorityQueue, canRunJobSet);
    }

    public String poll(Set<String> canRunJobSet) {
        return pollFromPriorityQueue(priorityQueue, canRunJobSet);
    }

    public void add(T priorityObject) {
        if (priorityObject.checkAddConstraint(systemStatus)) {
            if (StringUtils.isEmpty(objectClassName)) {
                objectClassName = priorityObject.getInstance().getName();
            }
            priorityQueue.add(priorityObject);
        }
    }

    /**
     * @return a list of tenantIds to run PA
     */
    public Set<String> getCanRunJobs(Set<String> canRunJobSet) {
        String tenantId;
        do {
            tenantId = poll(canRunJobSet);
            if (tenantId == null) {
                break;
            }
            canRunJobSet.add(tenantId);
        }while (true);
        return canRunJobSet;
    }


    private List<String> getAllMemberFromQueue(PriorityQueue<T> priorityQueue) {
        List<String> memberList = new ArrayList<>();
        List<T> priorityObjectList = new LinkedList<>();
        while (priorityQueue.peek() != null) {
            T priorityObject = priorityQueue.poll();
            priorityObjectList.add(priorityObject);
            memberList.add(priorityObject.getTenantActivity().getTenantId());
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
            if (tenantId.equalsIgnoreCase(priorityObject.getTenantActivity().getTenantId())) {
                priorityQueue.addAll(priorityObjectList);
                return index;
            }
            index++;
        }
        priorityQueue.addAll(priorityObjectList);
        return -1;
    }

    private String peekFromPriorityQueue(PriorityQueue<T> priorityQueue, Set<String> canRunJobSet) {
        T priorityObject = priorityQueue.peek();
        return (priorityObject != null && priorityObject.checkPopConstraint(systemStatus, canRunJobSet)) ?
                priorityObject.getTenantActivity().getTenantId() : null;
    }

    private String pollFromPriorityQueue(PriorityQueue<T> priorityQueue, Set<String> canRunJobSet) {
        T priorityObject = priorityQueue.peek();
        if (priorityObject != null && priorityObject.checkPopConstraint(systemStatus, canRunJobSet)) {
            systemStatus.changeSystemState(priorityObject.getTenantActivity());
            priorityQueue.poll();
            return priorityObject.getTenantActivity().getTenantId();
        }
        return null;
    }
}
