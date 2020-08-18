package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.scheduling.queue.SchedulingPAObject;
import com.latticeengines.domain.exposed.cdl.scheduling.queue.SchedulingPAQueue;

public class GreedyScheduler implements Scheduler {

    private static final Logger log = LoggerFactory.getLogger(GreedyScheduler.class);

    @Override
    public SchedulingResult schedule(List<SchedulingPAQueue> schedulingPAQueues) {
        Set<String> canRunRetryJobTenantSet = new HashSet<>();
        Set<String> canRunJobTenantSet = new HashSet<>();
        // queueName -> list(scheduling object)
        Map<String, SchedulingResult.Detail> details = new HashMap<>();
        Set<String> allTenantsInQ = new HashSet<>();
        Map<String, List<SchedulingResult.ConstraintViolationReason>> reasons = new HashMap<>();
        for (SchedulingPAQueue<?> schedulingPAQueue : schedulingPAQueues) {
            if (schedulingPAQueue.size() > 0) {
                allTenantsInQ.addAll(schedulingPAQueue.getAll());
                if (log.isDebugEnabled()) {
                    log.debug(String.format("queue %s shows : %s", schedulingPAQueue.getQueueName(),
                            JsonUtils.serialize(schedulingPAQueue.getAll())));
                }
                List<SchedulingPAObject> objs = schedulingPAQueue.fillAllCanRunJobs();
                details.putAll(getDetails(schedulingPAQueue.getQueueName(), objs, schedulingPAQueue.getTimeClock()));
                Set<String> tenantIds = SchedulingPAUtils.getTenantIds(objs);
                if (schedulingPAQueue.isRetryQueue()) {
                    canRunRetryJobTenantSet.addAll(tenantIds);
                } else {
                    canRunJobTenantSet.addAll(tenantIds);
                }

                // save violation reasons for each tenant
                Map<String, String> queueReasons = schedulingPAQueue.getConstraintViolationReasons();
                String queueName = schedulingPAQueue.getQueueName();
                if (MapUtils.isNotEmpty(queueReasons)) {
                    queueReasons.forEach((tenantId, reason) -> {
                        reasons.putIfAbsent(tenantId, new ArrayList<>());
                        reasons.get(tenantId).add(new SchedulingResult.ConstraintViolationReason(queueName, reason));
                    });
                }

                // log notification for now. TODO expose this info and raise alert in other ways
                Map<String, List<String>> notifications = schedulingPAQueue.getConstraintNotifications();
                MapUtils.emptyIfNull(notifications).forEach((tenantId, messages) -> {
                    String shortId = CustomerSpace.shortenCustomerSpace(tenantId);
                    messages.forEach(msg -> log.info("scheduler notification from queue {} for tenant {}: {}",
                            queueName, shortId, msg));
                });
            }
        }
        canRunJobTenantSet.removeAll(canRunRetryJobTenantSet);
        SchedulingResult result = new SchedulingResult(canRunJobTenantSet, canRunRetryJobTenantSet, details,
                allTenantsInQ, reasons);
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
                    clock.getCurrentTime() - firstActivityTime, activity, obj.getConsumedPAQuotaName());
            return Pair.of(detail.getTenantActivity().getTenantId(), detail);
        }).filter(Objects::nonNull).collect(Collectors.toMap(Pair::getKey, Pair::getValue, (v1, v2) -> v1));
    }
}
