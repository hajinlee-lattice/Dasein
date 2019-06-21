package com.latticeengines.domain.exposed.cdl;

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

    private final boolean isRetryQueue;

    public SchedulingPAQueue(SystemStatus systemStatus, Class<T> clz) {
        this(systemStatus, clz, false);
    }

    public SchedulingPAQueue(SystemStatus systemStatus, Class<T> clz, boolean isRetryQueue) {
        this.systemStatus = systemStatus;
        this.clz = clz;
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

    public String peek(Set<String> canRunJobSet) {
        return peekFromPriorityQueue(priorityQueue, canRunJobSet);
    }

    public String poll(Set<String> canRunJobSet) {
        return pollFromPriorityQueue(priorityQueue, canRunJobSet);
    }

    /**
     * According to pushConstraintList check the valid of the priority Object
     * @param priorityObject which want to push into queue.
     */
    public void add(T priorityObject) {
        if (checkConstraint(systemStatus, null, priorityObject.getTenantActivity(),
                priorityObject.getPushConstraints())) {
            priorityQueue.add(priorityObject);
        }
    }

    /**
     * Retrieve all tenants that should be scheduled jobs for and add them to the
     * input set.
     *
     * @param canRunJobSet
     *            set of tenants that we already decide to schedule job.
     * @return set of tenantIds to run PA for, all elements in this set will also be
     *         added to canRunJobSet
     */
    public Set<String> fillAllCanRunJobs(Set<String> canRunJobSet) {
        String tenantId;
        Set<String> canRunJobSetInQueue = new HashSet<>();
        // FIXME @Joy should NOT break when tenant is null since later tenants in queue
        // can still be scheduled. Better to use size() > 0 as while loop condition.
        do {
            tenantId = poll(canRunJobSet);
            if (tenantId == null) {
                break;
            }
            canRunJobSet.add(tenantId);
            canRunJobSetInQueue.add(tenantId);
        }while (true);
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
     * @param canRunJobSet which was used for check popConstraint
     * @return According to popConstraintList and canRunJobSet peek the priority Object which obey the Constraint.
     */
    private String peekFromPriorityQueue(PriorityQueue<T> priorityQueue, Set<String> canRunJobSet) {
        // FIXME @Joy you should peek the first object that doesn't violate constraint.
        // I would recommend just pop invalid objects since they'll be popped anyways.
        T priorityObject = priorityQueue.peek();
        return (priorityObject != null && checkConstraint(systemStatus, canRunJobSet,
                priorityObject.getTenantActivity(), priorityObject.getPopConstraints())) ?
                priorityObject.getTenantActivity().getTenantId() : null;
    }

    /**
     *
     * @param priorityQueue which contains all valid priority Object
     * @param canRunJobSet which was used for check popConstraint
     * @return According to popConstraintList and canRunJobSet pop the priority Object which obey the Constraint.
     */
    private String pollFromPriorityQueue(PriorityQueue<T> priorityQueue, Set<String> canRunJobSet) {
        // FIXME @Joy if some object violated constraint, it will never be popped.
        T priorityObject = priorityQueue.peek();
        if (priorityObject != null && checkConstraint(systemStatus, canRunJobSet, priorityObject.getTenantActivity(),
                priorityObject.getPopConstraints()
                )) {
            systemStatus.changeSystemState(priorityObject.getTenantActivity());
            priorityQueue.poll();
            return priorityObject.getTenantActivity().getTenantId();
        }
        return null;
    }

    /**
     * this method is used when schedulingPAObject push into queue (pop from queue). check if this object can push into
     * queue(pop from queue) or not.
     */
    private boolean checkConstraint(SystemStatus systemStatus, Set<String> scheduledTenants,
                                    TenantActivity tenantActivity,
                              List<Constraint> constraintList) {
        boolean violated = false;
        for (Constraint constraint : constraintList) {
            if (constraint.checkViolated(systemStatus, scheduledTenants, tenantActivity)) {
                violated = true;
                break;
            }
        }
        return !violated;
    }
}
