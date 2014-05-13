package com.latticeengines.dataplatform.service.impl;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerLeafQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Splitter;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.service.YarnQueueAssignmentService;

@Component("yarnQueueAssignmentService")
public class YarnQueueAssignmentServiceImpl implements YarnQueueAssignmentService {

    private static final String QUEUE_NAME_DELIMITER = ".";

    // Note: "finishing" is not an accepted state when querying for apps
    static final String APP_STATES_ACCEPTED_RUNNING = "states=accepted,running";

    private static final Log log = LogFactory.getLog(YarnQueueAssignmentServiceImpl.class);

    @Autowired
    private YarnService yarnService;

    private Splitter queueNameSplitter = Splitter.on(QUEUE_NAME_DELIMITER);
    
    public void setYarnService(YarnService yarnServiceToUse) {
        yarnService = yarnServiceToUse;
    }

    @Override
    public String getAssignedQueue(String customer, String requestedParentQueue) {
        if (log.isDebugEnabled()) {
            log.debug("getAssignedQueue: customer = " + customer + " parentQueue = " + requestedParentQueue);
        }

        String assignedQueue = null;
        assignedQueue = getStickyQueue(customer, requestedParentQueue);

        if (assignedQueue == null) {
            assignedQueue = getNewAssignedQueue(requestedParentQueue);
        }

        if (assignedQueue == null) {
            throw new LedpException(LedpCode.LEDP_12002, new String[] { requestedParentQueue });
        }
        return assignedQueue;
    }

    private String getStickyQueue(String customer, String requestedParentQueue) {
        String stickyQueue = null;

        AppsInfo apps = yarnService.getApplications(APP_STATES_ACCEPTED_RUNNING);

        // Ordering is not assumed in getApps response, therefore evaluate ALL apps
        for (AppInfo app : apps.getApps()) {
            if (app.getName().startsWith(customer)) {            
                List<String> queueNameSplits = queueNameSplitter.splitToList(app.getQueue());
                // Size-2 == location of parentQueue split
                if (queueNameSplits.get(queueNameSplits.size() - 2).equals(requestedParentQueue)) {
                    stickyQueue = app.getQueue();
                    break;
                }                                                     
            }
        }
        return stickyQueue;
    }

    private String getNewAssignedQueue(String requestedParentQueue) {
        FairSchedulerQueueInfo parentQueue = getParentQueue(requestedParentQueue); 
        if (parentQueue == null) {
            throw new LedpException(LedpCode.LEDP_12001, new String[] { requestedParentQueue});
        }
        if (parentQueue.getChildQueues() == null) {
            throw new LedpException(LedpCode.LEDP_12002, new String[] { requestedParentQueue });
        }        
        
        return getAssignedQueue(parentQueue);
    }

    private String getAssignedQueue(FairSchedulerQueueInfo parentQueue) {
        // First determine the minQueueUtlilization
        int minQueueUtilization = Integer.MAX_VALUE;
        final int LEAST_POSSIBLE_UTILIZATION = 0;
        for (FairSchedulerQueueInfo childQueue : parentQueue.getChildQueues()) {
            // Defensive; this condition should never be true since all queues beneath the parent should be leaves
            if (!(childQueue instanceof FairSchedulerLeafQueueInfo)) {                
                continue;
            }
            
            FairSchedulerLeafQueueInfo leafQueue = (FairSchedulerLeafQueueInfo) childQueue;
            int queueUtilization = leafQueue.getNumActiveApplications() + leafQueue.getNumPendingApplications();           
            
            // shortcircuit
            if (queueUtilization == LEAST_POSSIBLE_UTILIZATION) {
                minQueueUtilization = LEAST_POSSIBLE_UTILIZATION;
                break;
            }

            if (queueUtilization < minQueueUtilization) {
                minQueueUtilization = queueUtilization;
            }            
        }
        
        // Next get the list of queues at minQueueUtilization
        List<String> leastUtilizedQueues = new ArrayList<String>();
        for (FairSchedulerQueueInfo childQueue : parentQueue.getChildQueues()) {
            // Defensive; this condition should never be true since all queues beneath the parent should be leaves
            if (!(childQueue instanceof FairSchedulerLeafQueueInfo)) {                
                continue;
            }
    
            FairSchedulerLeafQueueInfo leafQueue = (FairSchedulerLeafQueueInfo) childQueue;
            int queueUtilization = leafQueue.getNumActiveApplications() + leafQueue.getNumPendingApplications();     
            
            if (queueUtilization == minQueueUtilization) {
                leastUtilizedQueues.add(childQueue.getQueueName());
            }        
        }
        
        int randomLeafQueueIndex = ThreadLocalRandom.current().nextInt(0, leastUtilizedQueues.size());
        
        return leastUtilizedQueues.get(randomLeafQueueIndex);
    }

    /**
     * @param requestedParentQueue This parameter should be unique among any queue (parent or leaf)
     */
    private FairSchedulerQueueInfo getParentQueue(String requestedParentQueue) {
        SchedulerTypeInfo schedulerInfo = yarnService.getSchedulerInfo();
        Field field;
        FairSchedulerQueueInfo parentQueue = null;
        try {
            field = SchedulerTypeInfo.class.getDeclaredField("schedulerInfo");
            field.setAccessible(true);
            FairSchedulerInfo fairSchedulerInfo = (FairSchedulerInfo) field.get(schedulerInfo);
            LinkedList<FairSchedulerQueueInfo> queuesToExplore = new LinkedList<FairSchedulerQueueInfo>();
            FairSchedulerQueueInfo rootQueue = fairSchedulerInfo.getRootQueueInfo();
            if (rootQueue != null) {
                queuesToExplore.add(rootQueue);
            }

            outer: while (!queuesToExplore.isEmpty()) {
                FairSchedulerQueueInfo curQueue = queuesToExplore.removeFirst();

                for (FairSchedulerQueueInfo childQueue : curQueue.getChildQueues()) {
                    if (childQueue.getQueueName().endsWith(requestedParentQueue)) {
                        parentQueue = childQueue;
                        break outer;
                    } else {
                        if (childQueue.getChildQueues() != null) {
                            queuesToExplore.addLast(childQueue);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Unable to process yarn queue state.", e);
            throw new LedpException(LedpCode.LEDP_00001);
        }

        return parentQueue;
    }
}
