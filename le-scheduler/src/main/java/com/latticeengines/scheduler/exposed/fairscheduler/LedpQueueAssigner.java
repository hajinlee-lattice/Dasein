package com.latticeengines.scheduler.exposed.fairscheduler;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.QueueManager;

import com.google.common.annotations.VisibleForTesting;

/**
 * An appIdToCustomer cache is required because I could find no other hook to
 * retrieve application names (not id's) for sticky queue name matching.
 *
 */
public class LedpQueueAssigner {

    private static final Log log = LogFactory.getLog(LedpQueueAssigner.class);

    public static final String PRIORITY = "Priority";

    private static final String MRQUEUENAME = "Priority0.MapReduce.0";
    private static final String JOBNAME_DELIMITER = "~";
    private static final String QUEUE_NAME_DELIMITER = ".";
    private static final int ABSOLUTE_MINIMUM_UTILIZATION = 0;
    private Map<String, String> appIdToCustomer = new HashMap<String, String>();

    public synchronized void removeApplication(ApplicationId applicationId) {
        appIdToCustomer.remove(applicationId.toString());
    }

    public synchronized String getAssignedQueue(String requestedQueue, RMApp rmApp, QueueManager queueManager) {
        appIdToCustomer.put(rmApp.getApplicationId().toString(), rmApp.getName().split(JOBNAME_DELIMITER)[0]);

        String requestedParentQueue = getParentQueueFromFullQueueName(requestedQueue);
        FSQueue parentQueue = getParentQueue(requestedParentQueue, queueManager);

        // In these error cases, just log and error and defer original
        // requestedParentQueue to Scheduler.
        if (parentQueue == null) {
            log.error("Requested parent queue:" + requestedParentQueue + " does not exist for analytics job:"
                    + rmApp.getName());
            return requestedParentQueue;
        }
        if (parentQueue.getChildQueues() == null || parentQueue.getChildQueues().isEmpty()) {
            log.error("No child queues exist in parent queue:" + parentQueue.getQueueName() + " for job:"
                    + rmApp.getName());
            return requestedParentQueue;
        }

        String assignedQueue = null;
        boolean isSticky = false;
        assignedQueue = getStickyQueue(rmApp, parentQueue);

        if (assignedQueue == null) {
            assignedQueue = getNewAssignedQueue(parentQueue);
        } else {
            isSticky = true;
        }

        if (log.isInfoEnabled()) {
            log.info(new StringBuilder("requestedParentQueue:").append(requestedParentQueue).append(" for ")
                    .append(rmApp.getName()).append("; assignedQueue:").append(assignedQueue).append(" isSticky:")
                    .append(isSticky));
        }

        return assignedQueue;
    }

    /**
     * @param requestedParentQueue
     *            This parameter should be unique among any queue (parent or
     *            leaf)
     */
    private FSQueue getParentQueue(String requestedParentQueue, QueueManager queueManager) {
        FSQueue parentQueue = null;
        parentQueue = queueManager.getQueue(requestedParentQueue);

        return parentQueue;
    }

    private String getStickyQueue(RMApp rmApp, FSQueue parentQueue) {
        String stickyQueue = null;

        String customerName = rmApp.getName().split(JOBNAME_DELIMITER)[0];

        outer: for (FSQueue childQueue : parentQueue.getChildQueues()) {
            // Currently, there is at least one case of a non-leaf child
            // (Priority0.MapReduce)
            if (!(childQueue instanceof FSLeafQueue)) {
                continue;
            }
            FSLeafQueue leafQueue = (FSLeafQueue) childQueue;

            for (FSAppAttempt appSched : leafQueue.getRunnableAppSchedulables()) {
                // appSched.getName() returns applicationId.toString()
                if (customerName.equals(appIdToCustomer.get(appSched.getName()))) {
                    stickyQueue = leafQueue.getQueueName();
                    break outer;
                }
            }
            for (FSAppAttempt appSched : leafQueue.getNonRunnableAppSchedulables()) {
                // appSched.getName() returns applicationId.toString()
                if (customerName.equals(appIdToCustomer.get(appSched.getName()))) {
                    stickyQueue = leafQueue.getQueueName();
                    break outer;
                }
            }
        }

        return stickyQueue;
    }

    private String getNewAssignedQueue(FSQueue parentQueue) {
        // First determine the minQueueUtilization
        int minQueueUtilization = Integer.MAX_VALUE;
        Map<Integer, FSLeafQueue> utilizationToLeafQueue = new HashMap<Integer, FSLeafQueue>();
        for (FSQueue childQueue : parentQueue.getChildQueues()) {
            // Currently, there is at least one case of a non-leaf child
            // (Priority0.MapReduce)
            if (!(childQueue instanceof FSLeafQueue)) {
                continue;
            }

            FSLeafQueue leafQueue = (FSLeafQueue) childQueue;
            int queueUtilization = leafQueue.getRunnableAppSchedulables().size()
                    + leafQueue.getNonRunnableAppSchedulables().size();

            if (queueUtilization < minQueueUtilization) {
                minQueueUtilization = queueUtilization;

                if (!utilizationToLeafQueue.containsKey(minQueueUtilization)) {
                    utilizationToLeafQueue.put(minQueueUtilization, leafQueue);
                }

                // Shortcircuit if min base case reached
                if (minQueueUtilization == ABSOLUTE_MINIMUM_UTILIZATION) {
                    break;
                }
            }
        }

        return utilizationToLeafQueue.get(minQueueUtilization).getQueueName();
    }

    @VisibleForTesting
    String getParentQueueFromFullQueueName(String fullQueueName) {
        int leafIndex = fullQueueName.lastIndexOf(QUEUE_NAME_DELIMITER);

        // Defensive check
        if (leafIndex <= 0) {
            return fullQueueName;
        } else {
            return fullQueueName.substring(0, leafIndex);
        }
    }

    public static String getNonMRQueueNameForSubmission(int priority) {
        return PRIORITY + priority + ".0";
    }

    public static String getMRQueueNameForSubmission() {
        return MRQUEUENAME;
    }
}
