package com.latticeengines.domain.exposed.cdl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GreedyScheduler implements Scheduler {
    @Override
    public List<String> schedule(List<SchedulingPAQueue> queues) {
        Set<String> scheduledTenants = new HashSet<>();
        for (SchedulingPAQueue q : queues) {
            q.getCanRunJobs(scheduledTenants);
        }
        return new ArrayList<>(scheduledTenants);
    }
}
