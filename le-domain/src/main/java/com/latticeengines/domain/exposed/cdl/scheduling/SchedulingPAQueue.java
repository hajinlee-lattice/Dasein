package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;


public class SchedulingPAQueue<T extends SchedulingPAObject> {

    private final SystemStatus systemStatus;

    private final PriorityQueue<T> priorityQueue;

    private final Class<T> clz;

    private final TimeClock timeClock;

    private final boolean isRetryQueue;

    public SchedulingPAQueue(SystemStatus systemStatus, Class<T> clz, TimeClock timeClock) {
        this(systemStatus, clz, timeClock, false);
    }

    public SchedulingPAQueue(SystemStatus systemStatus, Class<T> clz, TimeClock timeClock, boolean isRetryQueue) {
        this.systemStatus = systemStatus;
        this.clz = clz;
        this.timeClock = timeClock;
        this.isRetryQueue = isRetryQueue;
        priorityQueue = new PriorityQueue<>();
    }

    public List<String> getAll() {
        return getAllMemberFromQueue(priorityQueue);
    }

    public String getQueueName() {
        return clz.getName();
    }

    public boolean isRetryQueue() {
        return isRetryQueue;
    }

    /**
     * Return tenant location (index start at 1 instead of 0).
     *
     * @param tenantId
     *            which we want to find the position in current queue
     * @return the tenant location, if not find, return -1.
     */
    public int getPosition(String tenantId) {
        return getPositionFromQueue(priorityQueue, tenantId);
    }

    public String peek() {
        return peekFromPriorityQueue(priorityQueue);
    }

    public String poll() {
        return pollFromPriorityQueue(priorityQueue);
    }

    /**
     * According to pushConstraintList check the valid of the priority Object
     * @param priorityObject which want to push into queue.
     */
    public void add(T priorityObject) {
        if (checkConstraint(systemStatus, priorityObject.getTenantActivity(),
                priorityObject.getPushConstraints())) {
            priorityQueue.add(priorityObject);
        }
    }

    /**
     * Retrieve all tenants that should be scheduled jobs for and add them to the
     * input set.
     *
     * @return set of tenantIds to run PA for, all elements in this set will also be
     *         added to canRunJobSet
     */
    public Set<String> fillAllCanRunJobs() {
        String tenantId;
        Set<String> canRunJobSetInQueue = new HashSet<>();
        do {
            tenantId = poll();
            if (tenantId != null) {
                canRunJobSetInQueue.add(tenantId);
            }
        }while (size() > 0);
        systemStatus.getScheduleTenants().addAll(canRunJobSetInQueue);
        return canRunJobSetInQueue;
    }

    /**
     *
     * @return priorityQueue size.
     */
    public int size() {
        return priorityQueue.size();
    }

    /**
     * get all element from priorityQueue.
     */
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

    /*
     * helper to retrieve tenant location in given priority queue
     */
    private int getPositionFromQueue(PriorityQueue<T> priorityQueue, String tenantId) {
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

    /**
     *
     * @param priorityQueue which contains all valid priority Object
     * @return According to popConstraintList and canRunJobSet peek the priority Object which obey the Constraint.
     */
    private String peekFromPriorityQueue(PriorityQueue<T> priorityQueue) {
        T priorityObject = priorityQueue.peek();
        while (priorityObject != null && !checkConstraint(systemStatus,
                priorityObject.getTenantActivity(), priorityObject.getPopConstraints())) {
            priorityQueue.poll();
            priorityObject = priorityQueue.peek();
        }
        return priorityObject == null ? null : priorityObject.getTenantActivity().getTenantId();
    }

    /**
     *
     * @param priorityQueue which contains all valid priority Object
     * @return According to popConstraintList and canRunJobSet pop the priority Object which obey the Constraint.
     */
    private String pollFromPriorityQueue(PriorityQueue<T> priorityQueue) {
        T priorityObject = priorityQueue.poll();
        while (priorityObject != null && !checkConstraint(systemStatus,
                priorityObject.getTenantActivity(),
                priorityObject.getPopConstraints()
                )) {
            priorityObject = priorityQueue.poll();
        }
        if (priorityObject == null) {
            return null;
        }
        systemStatus.changeSystemState(priorityObject.getTenantActivity());
        return priorityObject.getTenantActivity().getTenantId();
    }

    /**
     * this method is used when schedulingPAObject push into queue (pop from queue). check if this object can push into
     * queue(pop from queue) or not.
     */
    private boolean checkConstraint(SystemStatus systemStatus,
                                    TenantActivity tenantActivity,
                              List<Constraint> constraintList) {
        boolean violated = false;
        for (Constraint constraint : constraintList) {
            if (constraint.checkViolated(systemStatus, tenantActivity, timeClock)) {
                violated = true;
                break;
            }
        }
        return !violated;
    }
}
