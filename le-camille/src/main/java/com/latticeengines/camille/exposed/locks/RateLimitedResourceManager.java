package com.latticeengines.camille.exposed.locks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.locks.RateLimitDefinition;
import com.latticeengines.domain.exposed.camille.locks.RateLimitedAcquisition;
import com.latticeengines.domain.exposed.camille.locks.RateLimitingStatus;

public class RateLimitedResourceManager {

    private static final String LOCK_NAME_PREFIX = "Resource_";
    private static final Log log = LogFactory.getLog(RateLimitedResourceManager.class);
    private static final double SAFETY_COEFFICIENT = 0.9;

    private static final ConcurrentMap<String, RateLimitDefinition> definitions = new ConcurrentHashMap<>();

    public static void registerResource(String resourceName, RateLimitDefinition definition) {
        synchronized (definitions) {
            if (definition.isCrossDivision()) {
                LockManager.registerCrossDivisionLock(toLockName(resourceName));
            } else {
                LockManager.registerDivisionPrivateLock(toLockName(resourceName));
            }
            definitions.put(resourceName, definition);
        }
    }

    // this method is only for testing
    public static void deregisterResource(String resourceName) {
        synchronized (definitions) {
            RateLimitDefinition definition = definitions.get(resourceName);
            if (definition.isCrossDivision()) {
                LockManager.registerCrossDivisionLock(toLockName(resourceName));
            } else {
                LockManager.deregisterDivisionPrivateLock(toLockName(resourceName));
            }
            definitions.remove(resourceName);
        }
    }

    public static RateLimitedAcquisition acquire(String resourceName, Map<String, Long> quantities, long duration,
            TimeUnit timeUnit) {
        if (!definitions.containsKey(resourceName)) {
            log.warn("The resource " + resourceName + " is not registered.");
            return RateLimitedAcquisition.disallowed()
                    .addRejectionReason("The resource " + resourceName + " is not registered.");
        }
        RateLimitDefinition definition = definitions.get(resourceName);
        Map<String, List<RateLimitDefinition.Quota>> quotas = definition.getQuotas();
        for (String counter : quantities.keySet()) {
            if (!quotas.containsKey(counter)) {
                log.error("There is no counter " + counter + " in the definition of " + resourceName);
                return RateLimitedAcquisition.disallowed().addRejectionReason(
                        "There is no counter " + counter + " in the definition of " + resourceName);
            }
        }

        RateLimitedAcquisition answerByPeek = isPossible(resourceName, quantities, duration, timeUnit);
        if (!answerByPeek.isAllowed()) {
            return answerByPeek;
        }

        return attemptToAcquire(resourceName, quantities, duration, timeUnit);
    }

    private static RateLimitedAcquisition isPossible(String resourceName, Map<String, Long> quantities, long duration,
            TimeUnit timeUnit) {
        RateLimitingStatus status;
        try {
            status = peekStatus(resourceName, duration, timeUnit);
            if (status == null) {
                throw new Exception("The parsed rate limiting status is null for " + resourceName);
            }
        } catch (Exception e) {
            log.error("Failed to peek rate limiting status for resource " + resourceName, e);
            return RateLimitedAcquisition.disallowed()
                    .addRejectionReason("Failed to peek rate limiting status : " + e.getMessage());
        }

        Map<String, Map<Long, Long>> history = status.getHistory();
        RateLimitDefinition definition = definitions.get(resourceName);
        Map<String, List<RateLimitDefinition.Quota>> quotas = definition.getQuotas();
        List<String> exceedingQuotas = new ArrayList<>();
        quantities.entrySet().forEach(entry -> {
            String counter = entry.getKey();
            quotas.get(counter).forEach(quota -> {
                String key = serializeCounterQuota(counter, quota);
                if (history.containsKey(key)) {
                    long inquiredQuantity = quantities.get(counter);
                    if (!quotaIsAvailable(inquiredQuantity, quota, history.get(key))) {
                        exceedingQuotas.add(serializeCounterQuota(counter, quota));
                    }
                }
            });
        });

        if (exceedingQuotas.isEmpty()) {
            return RateLimitedAcquisition.allowed(System.currentTimeMillis());
        } else {
            RateLimitedAcquisition answer = RateLimitedAcquisition.disallowed()
                    .addRejectionReason("Quotas will be exceeded if the present acquisition is allowed.");
            exceedingQuotas.forEach(answer::addExceedingQuota);
            return answer;
        }
    }

    private static RateLimitedAcquisition attemptToAcquire(String resourceName, Map<String, Long> quantities,
            long duration, TimeUnit timeUnit) {
        String lockName = toLockName(resourceName);
        try {
            if (!LockManager.acquireWriteLock(lockName, duration, timeUnit)) {
                return RateLimitedAcquisition.disallowed()
                        .addRejectionReason("Cannot acquire the write lock for resource " + resourceName);
            }
        } catch (Exception e) {
            log.error("Error when acquiring write lock for " + lockName, e);
            return RateLimitedAcquisition.disallowed().addRejectionReason(
                    "Error when acquiring the write lock for resource " + resourceName + " : " + e.getMessage());
        }

        try {
            RateLimitedAcquisition reconfirmByPeek = isPossible(resourceName, quantities, duration, timeUnit);
            if (!reconfirmByPeek.isAllowed()) {
                return reconfirmByPeek;
            }

            RateLimitingStatus status;
            try {
                status = peekStatus(resourceName, duration, timeUnit);
                if (status == null) {
                    throw new Exception("The parsed rate limiting status is null for " + resourceName);
                }
            } catch (Exception e) {
                log.error("Failed to peek rate limiting status for resource " + resourceName, e);
                return RateLimitedAcquisition.disallowed()
                        .addRejectionReason("Failed to peek rate limiting status : " + e.getMessage());
            }

            Map<String, Map<Long, Long>> history = status.getHistory();
            Map<String, Map<Long, Long>> newHistory = new HashMap<>();
            RateLimitDefinition definition = definitions.get(resourceName);
            Map<String, List<RateLimitDefinition.Quota>> quotas = definition.getQuotas();
            quantities.entrySet().forEach(entry -> {
                String counter = entry.getKey();
                quotas.get(counter).forEach(quota -> {
                    String key = serializeCounterQuota(counter, quota);
                    if (!history.containsKey(key)) {
                        history.put(key, new HashMap<>());
                    }
                    long inquiredQuantity = quantities.get(counter);
                    Map<Long, Long> newHistoryForCounter = updateHistory(inquiredQuantity, quota, history.get(key));
                    newHistory.put(key, newHistoryForCounter);
                });
            });

            RateLimitingStatus newStatus = new RateLimitingStatus();
            newStatus.setHistory(newHistory);

            try {
                String division = definition.isCrossDivision() ? "" : CamilleEnvironment.getDivision();
                LockManager.upsertData(lockName, JsonUtils.serialize(newStatus), division);
            } catch (Exception e) {
                log.error("Failed to update rate limiting status for " + resourceName, e);
                return RateLimitedAcquisition.disallowed()
                        .addRejectionReason("Failed to update rate limiting status : " + e.getMessage());
            }

            return RateLimitedAcquisition.allowed(System.currentTimeMillis());
        } finally {
            LockManager.releaseWriteLock(lockName);
        }
    }

    private static RateLimitingStatus peekStatus(String resourceName, long duration, TimeUnit timeUnit)
            throws Exception {
        String lockName = toLockName(resourceName);
        String data = LockManager.peekData(lockName, duration, timeUnit);
        if (StringUtils.isEmpty(data)) {
            return new RateLimitingStatus();
        } else {
            return JsonUtils.deserialize(data, RateLimitingStatus.class);
        }
    }

    private static boolean quotaIsAvailable(long inquiredQuantity, RateLimitDefinition.Quota quota,
            Map<Long, Long> history) {

        long totalHistoryQuantityWithinQuota = history.entrySet().stream()//
                .filter(entry -> //
                (System.currentTimeMillis() - entry.getKey()) < quota.getTimeUnit().toMillis(quota.getDuration())) //
                .reduce(0L, (acc, entry) -> acc + entry.getValue(), (acc1, acc2) -> acc1 + acc2);

        return totalHistoryQuantityWithinQuota + inquiredQuantity <= Math.max(SAFETY_COEFFICIENT * quota.getMaxQuantity(), 1);
    }

    private static Map<Long, Long> updateHistory(long inquiredQuantity, RateLimitDefinition.Quota quota,
            Map<Long, Long> history) {
        Map<Long, Long> newHistory = new HashMap<>();
        history.entrySet().stream()
                .filter( //
                        entry -> System.currentTimeMillis() - entry.getKey() < quota.getTimeUnit()
                                .toMillis(quota.getDuration())) //
                .forEach(entry -> newHistory.put(entry.getKey(), entry.getValue()));
        newHistory.put(System.currentTimeMillis(), inquiredQuantity);
        return newHistory;
    }

    public static String toLockName(String resourceName) {
        return LOCK_NAME_PREFIX + resourceName;
    }

    private static String serializeCounterQuota(String counter, RateLimitDefinition.Quota quota) {
        return counter + "_" + quota;
    }

}
