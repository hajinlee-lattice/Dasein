package com.latticeengines.domain.exposed.cdl;

import java.util.ArrayList;
import java.util.HashSet;
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

    /**
     * According to pushConstraintList check the valid of the priority Object
     * @param priorityObject which want to push into queue.
     */
    public void add(T priorityObject) {
        if (checkConstraint(systemStatus, null, priorityObject.getTenantActivity(),
                priorityObject.getPushConstraints())) {
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
        Set<String> canRunJobSetInQueue = new HashSet<>();
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

    /**
     *
     * @param priorityQueue which contains all valid priority Object
     * @param tenantId which we want to find the position in priority Queue
     * @return the tenant location in Queue, if not find, return -1.
     */
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

    /**
     *
     * @param priorityQueue which contains all valid priority Object
     * @param canRunJobSet which was used for check popConstraint
     * @return According to popConstraintList and canRunJobSet peek the priority Object which obey the Constraint.
     */
    private String peekFromPriorityQueue(PriorityQueue<T> priorityQueue, Set<String> canRunJobSet) {
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
