package com.latticeengines.domain.exposed.cdl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class GreedyScheduler implements Scheduler {
    @Override
    public List<String> schedule(List<SchedulingPAQueue> queues) {
        Set<String> scheduledTenants = null;
        for (SchedulingPAQueue<?> q : queues) {
            if (scheduledTenants == null) {
                scheduledTenants = q.getScheduleTenants();
            }
            q.fillAllCanRunJobs();
        }
        return new ArrayList<>(scheduledTenants);
    }
}
