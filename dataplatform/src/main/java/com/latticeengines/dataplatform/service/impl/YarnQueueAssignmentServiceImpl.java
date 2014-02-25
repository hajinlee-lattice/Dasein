package com.latticeengines.dataplatform.service.impl;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import net.jpountz.xxhash.XXHashFactory;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerLeafQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.service.YarnQueueAssignmentService;

@Component("yarnQueueAssignmentService")
public class YarnQueueAssignmentServiceImpl implements
        YarnQueueAssignmentService {

    private static final Log log = LogFactory
            .getLog(YarnQueueAssignmentServiceImpl.class);

    // XXX - initialize with 1K random permutations from 1 to 16
    private static Integer[][] randomPermutations = new Integer[][] {
            { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
            { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
            { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } };

    @Autowired
    private YarnService yarnService;

    @Override
    public String assignToQueue(String assignmentToken, AssignmentPolicy policy)
            throws UnsupportedEncodingException, NotImplementedException {

        log.debug("assignToQueue: assignmentToken=" + assignmentToken
                + " policy=" + policy.toString());
        if (policy != AssignmentPolicy.SHORTESTQUEUE) {
            throw new NotImplementedException("AssignmentPolicy policy "
                    + policy.toString() + " not implemented");
        }

        // Get current state of all leaf queues

        HashMap<Integer, Integer> curQueueDepth = new HashMap<Integer, Integer>();
        HashMap<Integer, String> fullyQualifiedQueueNames = new HashMap<Integer, String>();

        // XXX - handle capacity scheduler as well
        // XXX - remove assumption that leaf queues are uniquely named with
        // integer identifiers
        SchedulerTypeInfo schedulerInfo = yarnService.getSchedulerInfo();
        Field field;
        try {
            field = SchedulerTypeInfo.class.getDeclaredField("schedulerInfo");
            field.setAccessible(true);
            FairSchedulerInfo fairScheduler = (FairSchedulerInfo) field
                    .get(schedulerInfo);
            LinkedList<FairSchedulerQueueInfo> queuesToExplore = new LinkedList<FairSchedulerQueueInfo>();
            queuesToExplore.add(fairScheduler.getRootQueueInfo());
            while (!queuesToExplore.isEmpty()) {
                FairSchedulerQueueInfo curQueue = queuesToExplore.removeFirst();
                Iterator<FairSchedulerQueueInfo> iter = curQueue
                        .getChildQueues().iterator();
                while (iter.hasNext()) {
                    FairSchedulerQueueInfo queue = (FairSchedulerQueueInfo) iter
                            .next();
                    if (queue instanceof FairSchedulerLeafQueueInfo) {
                        String queueName = queue.getQueueName();
                        String[] queueNodes = queueName.split(".");
                        Integer leafQueueName = Integer
                                .parseInt(queueNodes[queueNodes.length - 1]);
                        Integer queueDepth = ((FairSchedulerLeafQueueInfo) queue)
                                .getNumActiveApplications()
                                + ((FairSchedulerLeafQueueInfo) queue)
                                        .getNumPendingApplications();
                        log.debug("Processing queue data: queueName="
                                + queueName + " leafQueueName=" + leafQueueName
                                + " queueDepth=" + queueDepth);
                        curQueueDepth.put(leafQueueName, queueDepth);
                        fullyQualifiedQueueNames.put(leafQueueName, queueName);
                    } else {
                        queuesToExplore.addLast(queue);
                    }
                }
            }
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (NoSuchFieldException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SecurityException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // Use assignment token to identify the permutation to use

        XXHashFactory factory = XXHashFactory.fastestInstance();
        byte[] data = assignmentToken.getBytes("UTF-8");
        // used to initialize the hash value, use whatever
        // value you want, but always the same
        Integer seed = 0x6741c180;
        Integer hash = factory.hash32().hash(data, 0, data.length, seed);
        Integer permutationId = hash % randomPermutations.length;

        // Use permutation to identify the queue to use as per the SHORTESTQUEUE
        // policy

        Integer minDepth = Integer.MAX_VALUE;
        Integer minDepthQueueId = -1;
        for (Integer i = 0; i < randomPermutations[permutationId].length; i++) {
            Integer queueId = randomPermutations[permutationId][i];
            if ((curQueueDepth.containsKey(queueId))
                    && (curQueueDepth.get(queueId) < minDepth)) {
                minDepth = curQueueDepth.get(queueId);
                minDepthQueueId = queueId;
                log.debug("New candidate queue: minDepth=" + minDepth
                        + " minDepthQueueId=" + minDepthQueueId);
            }
        }

        if (minDepthQueueId != -1) {
            return fullyQualifiedQueueNames.get(minDepthQueueId);
        } else {
            // XXX - handle with an exception
            return "";
        }
    }

}
