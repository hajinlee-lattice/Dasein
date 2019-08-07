package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;

public class GreedyScheduler implements Scheduler {

    private static final Logger log = LoggerFactory.getLogger(GreedyScheduler.class);

    @Override
    public SchedulingResult schedule(List<SchedulingPAQueue> schedulingPAQueues) {
        Set<String> canRunRetryJobTenantSet = new HashSet<>();
        Set<String> canRunJobTenantSet = new HashSet<>();
        // queueName -> list(scheduling object)
        Map<String, SchedulingResult.Detail> details = new HashMap<>();
        Set<String> allTenantsInQ = new HashSet<>();
        for (SchedulingPAQueue<?> schedulingPAQueue : schedulingPAQueues) {
            if (schedulingPAQueue.size() > 0) {
                allTenantsInQ.addAll(schedulingPAQueue.getAll());
                if (log.isDebugEnabled()) {
                    log.debug(String.format("queue %s shows : %s", schedulingPAQueue.getQueueName(),
                            JsonUtils.serialize(schedulingPAQueue.getAll())));
                }
                List<SchedulingPAObject> objs = schedulingPAQueue.fillAllCanRunJobs();
                details.putAll(getDetails(schedulingPAQueue.getQueueName(), objs, schedulingPAQueue.getTimeClock()));
                Set<String> tenantIds = SchedulingPAUtil.getTenantIds(objs);
                if (schedulingPAQueue.isRetryQueue()) {
                    canRunRetryJobTenantSet.addAll(tenantIds);
                } else {
                    canRunJobTenantSet.addAll(tenantIds);
                }
            }
        }
        canRunJobTenantSet.removeAll(canRunRetryJobTenantSet);
        SchedulingResult result = new SchedulingResult(canRunJobTenantSet, canRunRetryJobTenantSet, details, allTenantsInQ);
        if (log.isDebugEnabled()) {
            log.debug("SchedulingResult = {}", JsonUtils.serialize(result));
        }
        return result;
    }

    private Map<String, SchedulingResult.Detail> getDetails(String queueName, List<SchedulingPAObject> schedulingObjs,
            TimeClock clock) {
        if (queueName == null || CollectionUtils.isEmpty(schedulingObjs) || clock == null) {
            return Collections.emptyMap();
        }

        return schedulingObjs.stream().map(obj -> {
            if (obj == null || obj.getTenantActivity() == null || obj.getTenantActivity().getTenantId() == null) {
                return null;
            }

            TenantActivity activity = obj.getTenantActivity();
            long firstActivityTime = clock.getCurrentTime();
            if (activity.getFirstActionTime() != null && activity.isAutoSchedule()) {
                firstActivityTime = Math.min(activity.getFirstActionTime(), firstActivityTime);
            }
            if (activity.getScheduleTime() != null && activity.isScheduledNow()) {
                firstActivityTime = Math.min(activity.getScheduleTime(), firstActivityTime);
            }
            if (activity.isRetry() && activity.getLastFinishTime() != null) {
                firstActivityTime = Math.min(activity.getLastFinishTime(), firstActivityTime);
            }

            SchedulingResult.Detail detail = new SchedulingResult.Detail(queueName,
                    clock.getCurrentTime() - firstActivityTime, activity);
            return Pair.of(detail.getTenantActivity().getTenantId(), detail);
        }).filter(Objects::nonNull).collect(Collectors.toMap(Pair::getKey, Pair::getValue, (v1, v2) -> v1));
    }
}
