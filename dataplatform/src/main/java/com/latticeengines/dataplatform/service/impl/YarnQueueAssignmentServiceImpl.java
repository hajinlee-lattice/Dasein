package com.latticeengines.dataplatform.service.impl;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.HashMap;

import net.jpountz.xxhash.XXHashFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.NotImplementedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.service.YarnQueueAssignmentService;
import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.util.Permutations;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerLeafQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;

@Component("yarnQueueAssignmentService")
public class YarnQueueAssignmentServiceImpl implements YarnQueueAssignmentService {

    private static final Log log = LogFactory.getLog(YarnQueueAssignmentServiceImpl.class);
    
    @Autowired
    private YarnService yarnService;
    
    private static HashMap<Integer, Integer> curQueueDepth = new HashMap<Integer, Integer>();
    private static HashMap<Integer, String> fullyQualifiedQueueNames = new HashMap<Integer, String>();
    private static HashMap<String, String> curQueueAssignment = new HashMap<String, String>();
    private static long queueStateLastUpdated = System.currentTimeMillis();

    public void setYarnService(YarnService yarnServiceToUse) {
        yarnService = yarnServiceToUse;
    }
    
    // Get current state of all queues
    private void updateCurrentQueueState() {
        // Get current depth of all leaf queues
        curQueueDepth.clear();
        fullyQualifiedQueueNames.clear();
        // XXX - handle capacity scheduler as well
        // XXX - remove assumption that leaf queues are uniquely named with integer identifiers
        SchedulerTypeInfo schedulerInfo = yarnService.getSchedulerInfo();
        Field field;
        try {
            field = SchedulerTypeInfo.class.getDeclaredField("schedulerInfo");
            field.setAccessible(true);
            FairSchedulerInfo fairScheduler = (FairSchedulerInfo) field
                    .get(schedulerInfo);
            LinkedList<FairSchedulerQueueInfo> queuesToExplore = new LinkedList<FairSchedulerQueueInfo>();
            FairSchedulerQueueInfo rootQueue = fairScheduler.getRootQueueInfo();
            if (rootQueue != null)
            {
                queuesToExplore.add (rootQueue);
            }
            while (!queuesToExplore.isEmpty()) {
                FairSchedulerQueueInfo curQueue = queuesToExplore.removeFirst();
                Iterator<FairSchedulerQueueInfo> iter = curQueue.getChildQueues()
                        .iterator();
                while (iter.hasNext()) {
                    FairSchedulerQueueInfo queue = (FairSchedulerQueueInfo) iter.next();                    
                    if (queue instanceof FairSchedulerLeafQueueInfo) {
                        String queueName = queue.getQueueName();
                        Integer leafQueueName = -1;
                        try {
                            String[] queueNodes = queueName.split("\\.");
                            leafQueueName = Integer.parseInt(queueNodes[queueNodes.length-1]);
                        }
                        catch (Exception e) {
                            log.error("unable to process queueName " + queueName);
                        }
                        Integer queueDepth = ((FairSchedulerLeafQueueInfo) queue).getNumActiveApplications() +
                                ((FairSchedulerLeafQueueInfo) queue).getNumPendingApplications();
                        log.debug("Processing queue data: queueName=" + queueName + 
                                  " leafQueueName=" + leafQueueName + 
                                  " queueDepth=" + queueDepth);
                        if (leafQueueName != -1) {
                            curQueueDepth.put(leafQueueName, queueDepth);
                            fullyQualifiedQueueNames.put(leafQueueName, queueName);
                        }
                    }
                    else {
                        queuesToExplore.addLast(queue);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Unable to process Yarn queue state", e);
            throw new LedpException(LedpCode.LEDP_00001);
        }        
        
        // Get current queue assignment for all customers
        curQueueAssignment.clear();
        AppsInfo apps = yarnService.getApplications("states=running,accepted");
        for (AppInfo app : apps.getApps())
        {
            String appName = app.getName();
            String[]appNameSubstrings = appName.split("\\.");
            if (!curQueueAssignment.containsKey(appNameSubstrings[0]))
            {
                curQueueAssignment.put(appNameSubstrings[0], app.getQueue());
            }
        }
        
        queueStateLastUpdated = System.currentTimeMillis();
    }
    
    private String shortestQueueAssignment(String assignmentToken) {
        
        // Use assignment token to identify the permutation to use
        
        XXHashFactory factory = XXHashFactory.fastestInstance();

        byte[] data; 
        try {
            data = assignmentToken.getBytes("UTF-8");
        } catch (Exception e) {
            log.error("Unable to convert assignmentToken to UTF-8 bytes", e);
            throw new LedpException(LedpCode.LEDP_00002);        
        }
        
        // used to initialize the hash value, use whatever
        // value you want, but always the same
        Integer seed = 0x6741c180; 
        Integer hash = Math.abs(factory.hash32().hash(data, 0, data.length, seed));
        Permutations permutations = Permutations.getInstance();
        Integer permutationId = hash % permutations.getNumPermutations();

        // Use permutation to identify the queue to use as per the SHORTESTQUEUE policy
        
        Integer minDepth = Integer.MAX_VALUE;
        Integer minDepthQueueId = -1;
        for (Integer i=0; i < permutations.getPermutationSize(); i++) {
            Integer queueId = permutations.getPermutationElement(permutationId,i);
            if ((curQueueDepth.containsKey(queueId)) && (curQueueDepth.get(queueId) < minDepth)) {
                minDepth = curQueueDepth.get(queueId);
                minDepthQueueId = queueId;
                log.debug("New candidate queue: minDepth=" + minDepth + " minDepthQueueId=" + minDepthQueueId);
            }
        }
                
        if (minDepthQueueId != -1) {
            return fullyQualifiedQueueNames.get(minDepthQueueId);
        }
        else {
            throw new LedpException(LedpCode.LEDP_12002);
        }
    }

    @Override
    public String useQueue(String assignmentToken, 
            AssignmentPolicy policy) throws NotImplementedException {
        return useQueue(assignmentToken, policy, true);
    }

    @Override
    public String useQueue(String assignmentToken, 
                                AssignmentPolicy policy,
                                Boolean refreshQueueState) throws NotImplementedException {
        
        log.debug("assignToQueue: assignmentToken=" + assignmentToken + " policy=" + policy.toString() + " refreshQueueState=" + refreshQueueState);
        if (policy != AssignmentPolicy.STICKYSHORTESTQUEUE) {
            throw new LedpException(LedpCode.LEDP_12001);
        }
        
        // XXX - make 60s settable via properties file
        if (((queueStateLastUpdated + 60*1000) < System.currentTimeMillis()) || (refreshQueueState == true)) {
            // Update current queue state
            updateCurrentQueueState();
        }

        if (policy == AssignmentPolicy.STICKYSHORTESTQUEUE) {
            // Check for a current queue assignment
            if (curQueueAssignment.containsKey(assignmentToken)) {
                return curQueueAssignment.get(assignmentToken);
            } else {
                // Use a new queue
                return shortestQueueAssignment(assignmentToken);
            }
        } else if (policy == AssignmentPolicy.SHORTESTQUEUE) {
            return shortestQueueAssignment(assignmentToken);
        }
    
        throw new LedpException(LedpCode.LEDP_12002);
    }
    
}
