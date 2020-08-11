package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.scheduling.queue.SchedulingPAQueue;

public interface Scheduler {
    /**
     * Take a list of scheduling PA queues and returns a list of tenantIds to run PA for
     */
    SchedulingResult schedule(List<SchedulingPAQueue> queues);
}
