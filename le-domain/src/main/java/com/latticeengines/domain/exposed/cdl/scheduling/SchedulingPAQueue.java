package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.Constraint;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.ConstraintValidationResult;


public class SchedulingPAQueue<T extends SchedulingPAObject> {

    private static final Logger log = LoggerFactory.getLogger(SchedulingPAQueue.class);

    private final SystemStatus systemStatus;

    private final PriorityQueue<T> priorityQueue;

    private final Class<T> clz;

    private final TimeClock timeClock;

    private final boolean isRetryQueue;

    private final String queueName;

    // tenant ID -> reason
    private final Map<String, String> constraintViolationReasons;

    public SchedulingPAQueue(SystemStatus systemStatus, Class<T> clz, TimeClock timeClock, String queueName) {
        this(systemStatus, clz, timeClock, false, queueName);
    }

    public SchedulingPAQueue(SystemStatus systemStatus, Class<T> clz, TimeClock timeClock, boolean isRetryQueue,
                             String queueName) {
        this.systemStatus = systemStatus;
        this.clz = clz;
        this.timeClock = timeClock;
        this.isRetryQueue = isRetryQueue;
        this.queueName = queueName;
        priorityQueue = new PriorityQueue<>();
        constraintViolationReasons = new HashMap<>();
    }

    public List<String> getAll() {
        return getAllMemberFromQueue(priorityQueue);
    }

    public String getQueueName() {
        return this.queueName;
    }

    public boolean isRetryQueue() {
        return isRetryQueue;
    }

    public TimeClock getTimeClock() {
        return timeClock;
    }

    public Map<String, String> getConstraintViolationReasons() {
        return constraintViolationReasons;
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

    public T peek() {
        return peekFromPriorityQueue(priorityQueue);
    }

    public T poll() {
        return pollFromPriorityQueue(priorityQueue);
    }

    /**
     * According to pushConstraintList check the valid of the priority Object
     * @param priorityObject which want to push into queue.
     */
    public void add(T priorityObject) {
        if (checkConstraint(systemStatus, priorityObject.getTenantActivity(),
                priorityObject.getPushConstraints(), false)) {
            priorityQueue.add(priorityObject);
        }
    }

    /**
     * Retrieve all tenants that should be scheduled jobs for and add them to the
     * input set.
     *
     * @return set of {@link SchedulingPAObject} for tenants to run PA for, all
     *         elements in this set will also be added to canRunJobSet
     */
    public List<SchedulingPAObject> fillAllCanRunJobs() {
        String tenantId;
        List<SchedulingPAObject> canRunJobSetInQueue = new ArrayList<>();
        Set<String> canRunJobTenants = new HashSet<>();
        do {
            SchedulingPAObject obj = poll();
            tenantId = SchedulingPAUtil.getTenantId(obj);
            if (tenantId != null) {
                canRunJobSetInQueue.add(obj);
                canRunJobTenants.add(tenantId);
            }
        }while (size() > 0);
        systemStatus.getScheduleTenants().addAll(canRunJobTenants);
        log.debug("queue: " + this.getQueueName() + ", canRunJobs: " + JsonUtils.serialize(canRunJobSetInQueue));
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
    private T peekFromPriorityQueue(PriorityQueue<T> priorityQueue) {
        T priorityObject = priorityQueue.peek();
        while (priorityObject != null && !checkConstraint(systemStatus,
                priorityObject.getTenantActivity(), priorityObject.getPopConstraints(), false)) {
            priorityQueue.poll();
            priorityObject = priorityQueue.peek();
        }
        return priorityObject;
    }

    /**
     *
     * @param priorityQueue which contains all valid priority Object
     * @return According to popConstraintList and canRunJobSet pop the priority Object which obey the Constraint.
     */
    private T pollFromPriorityQueue(PriorityQueue<T> priorityQueue) {
        T priorityObject = priorityQueue.poll();
        while (priorityObject != null && !checkConstraint(systemStatus, priorityObject.getTenantActivity(),
                priorityObject.getPopConstraints(), true)) {
            priorityObject = priorityQueue.poll();
        }
        if (priorityObject == null) {
            return null;
        }
        systemStatus.changeSystemState(priorityObject.getTenantActivity());
        return priorityObject;
    }

    /**
     * this method is used when schedulingPAObject push into queue (pop from queue). check if this object can push into
     * queue(pop from queue) or not.
     */
    private boolean checkConstraint(SystemStatus systemStatus, TenantActivity tenantActivity,
            List<Constraint> constraintList, boolean recordViolationReason) {
        boolean violated = false;
        for (Constraint constraint : constraintList) {
            ConstraintValidationResult result = constraint.validate(systemStatus, tenantActivity, timeClock);
            if (result.isViolated()) {
                violated = true;
                if (recordViolationReason && StringUtils.isNotBlank(result.getReason())) {
                    constraintViolationReasons.put(tenantActivity.getTenantId(), result.getReason());
                }
                break;
            }
        }
        return !violated;
    }
}
