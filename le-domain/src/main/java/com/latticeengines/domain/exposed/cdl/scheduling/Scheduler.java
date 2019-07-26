package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.List;

public interface Scheduler {
    /**
     * Take a list of scheduling PA queues and returns a list of tenantIds to run PA for
     */
    SchedulingResult schedule(List<SchedulingPAQueue> queues);
}
