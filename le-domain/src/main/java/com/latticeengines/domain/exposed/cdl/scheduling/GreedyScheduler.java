package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;

public class GreedyScheduler implements Scheduler {

    private static final Logger log = LoggerFactory.getLogger(GreedyScheduler.class);

    @Override
    public Map<String, Set<String>> schedule(List<SchedulingPAQueue> schedulingPAQueues) {
        Set<String> canRunRetryJobTenantSet = new HashSet<>();

        Set<String> canRunJobTenantSet = new HashSet<>();
        for (SchedulingPAQueue<?> schedulingPAQueue : schedulingPAQueues) {
            if (schedulingPAQueue.size() > 0) {
                log.debug(String.format("queue %s shows : %s", schedulingPAQueue.getQueueName(),
                        JsonUtils.serialize(schedulingPAQueue.getAll())));
                if (schedulingPAQueue.isRetryQueue()) {
                    canRunRetryJobTenantSet = new HashSet<>(schedulingPAQueue.fillAllCanRunJobs());
                } else {
                    canRunJobTenantSet.addAll(schedulingPAQueue.fillAllCanRunJobs());
                }
            }
        }
        Map<String, Set<String>> canRunJobTenantMap = new HashMap<>();
        canRunJobTenantMap.put(RETRY_KEY, canRunRetryJobTenantSet);
        log.debug(JsonUtils.serialize(canRunJobTenantMap));
        canRunJobTenantSet.removeAll(canRunRetryJobTenantSet);
        canRunJobTenantMap.put(OTHER_KEY, canRunJobTenantSet);
        log.debug(JsonUtils.serialize(canRunJobTenantMap));
        log.info("can run PA job tenant list is : " + JsonUtils.serialize(canRunJobTenantMap));
        return canRunJobTenantMap;
    }
}
