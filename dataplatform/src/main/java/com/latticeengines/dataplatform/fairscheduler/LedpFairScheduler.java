package com.latticeengines.dataplatform.fairscheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

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
    private static final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();
    private static final Resource clusterCapacity = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Resource.class);

    @Override
    protected Resource resToPreempt(FSLeafQueue sched, long curTime) {
        if (isP0(sched)) {
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

    @Override
    protected void preemptResources(Collection<FSLeafQueue> scheds, Resource toPreempt) {
        Resource resToPreempt = Resource.newInstance(toPreempt.getMemory(), toPreempt.getVirtualCores());
        if (canPreemptUsingNonP0Resources(scheds, resToPreempt)) {
            Collection<FSLeafQueue> noP0Scheds = new ArrayList<FSLeafQueue>();

            for (FSLeafQueue sched : scheds) {
                if (!isP0(sched)) {
                    noP0Scheds.add(sched);
                }
            }
            
            super.preemptResources(noP0Scheds, toPreempt);
        } else {
            super.preemptResources(scheds, toPreempt);
        }
    }
    
    private boolean isP0(FSLeafQueue queue) {
        return queue.getQueueName().contains("Priority0");
    }

}
