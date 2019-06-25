package com.latticeengines.domain.exposed.cdl;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Scheduler {

    /**
     * Take a list of scheduling PA queues and returns a list of tenantIds to run PA for
     */
    Map<String, Set<String>> schedule(List<SchedulingPAQueue> queues);
}
