package com.latticeengines.domain.exposed.cdl;

import java.util.List;

public interface Scheduler {

    /**
     * Take a list of scheduling PA queues and returns a list of tenantIds to run PA for
     */
    List<String> schedule(List<SchedulingPAQueue> queues);
}
