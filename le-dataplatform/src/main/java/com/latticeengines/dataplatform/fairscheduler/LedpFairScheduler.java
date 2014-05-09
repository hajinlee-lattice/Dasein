package com.latticeengines.dataplatform.fairscheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AppSchedulable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

public class LedpFairScheduler extends FairScheduler {
    private static final Log log = LogFactory.getLog(LedpFairScheduler.class);
    private static final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();
    private static final Resource clusterCapacity = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Resource.class);

    @Override
    protected Resource resToPreempt(FSLeafQueue sched, long curTime) {
        if (isP0(sched)) {
            Resource targetForMinShare = Resources.min(resourceCalculator, clusterCapacity,
                    sched.getMinShare(), sched.getDemand());
            Resource targetForFairShare = Resources.min(resourceCalculator, clusterCapacity,
                    sched.getFairShare(), sched.getDemand());
            Resource resDueToMinShare = Resources.max(resourceCalculator, clusterCapacity,
                    Resources.none(), Resources.subtract(targetForMinShare, sched.getResourceUsage()));
            Resource resDueToFairShare = Resources.max(resourceCalculator, clusterCapacity,
                    Resources.none(), Resources.subtract(targetForFairShare, sched.getResourceUsage()));
            
            String msg = "demand = " + sched.getDemand() + " targetForMinShare = " + targetForMinShare + " resDueToMinShare = " + resDueToMinShare
                    + " targetForFairShare = " + targetForFairShare + " resDueToFairShare = " + resDueToFairShare;
            log.info(msg);
            return super.resToPreempt(sched, curTime);
        }
        return Resources.none();
    }

    private boolean canPreemptUsingNonP0Resources(Collection<FSLeafQueue> scheds, Resource toPreempt) {
        List<RMContainer> runningContainers = new ArrayList<RMContainer>();
        for (FSLeafQueue sched : scheds) {
            if (isP0(sched)) {
                continue;
            }
            for (AppSchedulable as : sched.getAppSchedulables()) {
                for (RMContainer c : as.getApp().getLiveContainers()) {
                    runningContainers.add(c);
                }
            }

        }

        // Sort containers into reverse order of priority
        Collections.sort(runningContainers, new Comparator<RMContainer>() {
            public int compare(RMContainer c1, RMContainer c2) {
                int ret = c1.getContainer().getPriority().compareTo(c2.getContainer().getPriority());
                if (ret == 0) {
                    return c2.getContainerId().compareTo(c1.getContainerId());
                }
                return ret;
            }
        });

        for (RMContainer container : runningContainers) {
            Resources.subtractFrom(toPreempt, container.getContainer().getResource());
        }
        

        if (Resources.greaterThan(resourceCalculator, clusterCapacity, toPreempt, Resources.none())) {
            return false;
        }

        return true;
    }

//    @Override
//    protected void preemptResources(Collection<FSLeafQueue> scheds, Resource toPreempt) {
//        Resource resToPreempt = Resource.newInstance(toPreempt.getMemory(), toPreempt.getVirtualCores());
//        Collection<FSLeafQueue> mrScheds = new ArrayList<FSLeafQueue>();
//
//        for (FSLeafQueue sched : scheds) {
//            if (!isMapReduce(sched)) {
//                mrScheds.add(sched);
//            }
//        }
//        
//        
//        if (canPreemptUsingNonP0Resources(mrScheds, resToPreempt)) {
//            Collection<FSLeafQueue> noP0Scheds = new ArrayList<FSLeafQueue>();
//
//            for (FSLeafQueue sched : mrScheds) {
//                if (!isP0(sched)) {
//                    noP0Scheds.add(sched);
//                }
//            }
//            
//            super.preemptResources(noP0Scheds, toPreempt);
//        } else {
//            super.preemptResources(mrScheds, toPreempt);
//        }
//    }
    
    private boolean isP0(FSLeafQueue queue) {
        return queue.getQueueName().contains("Priority0");
    }
    
    private boolean isMapReduce(FSLeafQueue queue) {
        return queue.getQueueName().contains("MapReduce");
    }


}
