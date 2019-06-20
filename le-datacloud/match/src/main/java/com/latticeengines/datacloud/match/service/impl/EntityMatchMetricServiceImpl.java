package com.latticeengines.datacloud.match.service.impl;

import java.time.Duration;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.context.annotation.Lazy;
import org.springframework.retry.RetryContext;
import org.springframework.stereotype.Component;

import com.github.benmanes.caffeine.cache.Cache;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.metric.FuzzyMatchHistory;
import com.latticeengines.datacloud.match.service.EntityMatchMetricService;
import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import com.latticeengines.domain.exposed.actors.VisitingHistory;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.monitor.exposed.service.MeterRegistryFactoryService;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;

@Lazy
@Component("entityMatchMetricService")
public class EntityMatchMetricServiceImpl implements EntityMatchMetricService {

    private static final String BATCH_MATCH_MODE = MatchActorSystem.BATCH_MODE;

    /*
     * metric names
     */
    private static final String METRIC_ACTOR_VISIT = "match.entity.actor.microengine";
    private static final String METRIC_HISTORY = "match.entity.history";
    private static final String METRIC_TRAVEL_ERROR = "match.entity.travel.error";
    private static final String METRIC_NUM_TRIES = "match.entity.num.tries";
    private static final String METRIC_DISTRIBUTION_RETRY = "match.entity.num.tries.dist.retry";
    private static final String METRIC_HAVE_RETRY_NUM_TRIES = "match.entity.num.tries.count.retry";
    private static final String METRIC_DYNAMO_THROTTLE = "match.entity.dynamo.throttling.count";
    private static final String METRIC_DYNAMO_CALL_ERROR_DIST = "match.entity.dynamo.call.error.dist";
    private static final String METRIC_DYNAMO_CALL_THROTTLE_DIST = "match.entity.dynamo.call.throttling.dist";
    private static final String METRIC_DYNAMO_CALL_RETRY_DIST = "match.entity.dynamo.call.retry.dist";
    private static final String METRIC_LOOKUP_CACHE = "match.entity.cache.lookup";
    private static final String METRIC_SEED_CACHE = "match.entity.cache.seed";

    /*
     * tag names (TODO probably move to a unified constant class)
     */
    private static final String TAG_ACTOR = "Actor";
    private static final String TAG_ENTITY = "Entity";
    private static final String TAG_TENANT = "Tenant";
    private static final String TAG_MATCH_MODE = "MatchMode";
    private static final String TAG_MATCHED = "Matched";
    private static final String TAG_ALLOCATE_ID_MODE = "AllocateId";
    private static final String TAG_ENV = "Environment";
    private static final String TAG_DYNAMO_TABLE = "Table";
    private static final String TAG_HAS_ERROR = "HasError";

    @Lazy
    @Inject
    private MeterRegistryFactoryService registryFactory;

    @Inject
    private MatchActorSystem matchActorSystem;

    @Override
    public void recordDynamoThrottling(EntityMatchEnvironment env, String tableName) {
        if (env == null || tableName == null) {
            return;
        }
        Counter.builder(METRIC_DYNAMO_THROTTLE) //
                .tag(TAG_ENV, env.name()) //
                .tag(TAG_DYNAMO_TABLE, tableName) //
                .register(registryFactory.getServiceLevelRegistry()) //
                .increment();
    }

    @Override
    public void recordDynamoCall(EntityMatchEnvironment env, String tableName, RetryContext context,
            boolean isThrottled) {
        if (context == null || env == null || tableName == null) {
            return;
        }

        recordDynamoDistri(METRIC_DYNAMO_CALL_ERROR_DIST, env, tableName, context.isExhaustedOnly());
        recordDynamoDistri(METRIC_DYNAMO_CALL_THROTTLE_DIST, env, tableName, isThrottled);
        DistributionSummary.builder(METRIC_DYNAMO_CALL_RETRY_DIST) //
                .tag(TAG_ENV, env.name()) //
                .tag(TAG_DYNAMO_TABLE, tableName) //
                .register(registryFactory.getServiceLevelRegistry()) //
                .record(context.getRetryCount());
    }

    @Override
    public void recordActorVisit(MatchTraveler traveler, VisitingHistory history) {
        if (traveler == null || history == null || traveler.getMatchInput() == null) {
            return;
        }
        if (!Boolean.FALSE.equals(history.getRejected())) {
            // not recording rejected visit
            return;
        }
        String tenantId = getTenantId(traveler);
        if (!shouldRecord(tenantId, traveler)) {
            return;
        }

        Timer.builder(METRIC_ACTOR_VISIT) //
                .tag(TAG_ACTOR, history.getSite()) //
                .tag(TAG_ENTITY, traveler.getEntity()) //
                .tag(TAG_MATCH_MODE, history.getActorSystemMode()) //
                .tag(TAG_ALLOCATE_ID_MODE, String.valueOf(traveler.getMatchInput().isAllocateId())) //
                .tag(TAG_TENANT, tenantId) //
                .register(registryFactory.getServiceLevelRegistry()) //
                .record(Duration.ofMillis(history.getDuration()));
    }

    @Override
    public void recordMatchHistory(FuzzyMatchHistory history) {
        if (history == null || history.getFact() == null || history.getFact().getMatchInput() == null) {
            return;
        }
        MatchTraveler traveler = history.getFact();
        if (traveler.getTotalTravelTime() == null || traveler.isMatched() == null) {
            return;
        }
        String tenantId = getTenantId(traveler);
        if (!shouldRecord(tenantId, traveler)) {
            return;
        }

        MeterRegistry rootRegistry = registryFactory.getServiceLevelRegistry();
        Timer.builder(METRIC_HISTORY) //
                .tag(TAG_ENTITY, traveler.getEntity()) //
                .tag(TAG_MATCH_MODE, traveler.getMode()) //
                .tag(TAG_MATCHED, String.valueOf(traveler.getResult() != null)) //
                .tag(TAG_ALLOCATE_ID_MODE, String.valueOf(traveler.getMatchInput().isAllocateId())) //
                .tag(TAG_TENANT, tenantId) //
                .register(rootRegistry) //
                .record(Duration.ofMillis(traveler.getTotalTravelTime().longValue()));
        boolean hasTravelError = traveler.getTravelException() != null;
        DistributionSummary.builder(METRIC_TRAVEL_ERROR) //
                .tag(TAG_ENTITY, traveler.getEntity()) //
                .tag(TAG_MATCH_MODE, traveler.getMode()) //
                .tag(TAG_TENANT, tenantId) //
                .tag(TAG_HAS_ERROR, String.valueOf(hasTravelError)) //
                .register(rootRegistry) //
                .record(hasTravelError ? 1.0 : 0.0);

        if (BATCH_MATCH_MODE.equalsIgnoreCase(traveler.getMode()) && traveler.getMatchInput().isAllocateId()) {
            int numTries = traveler.getRetries();
            DistributionSummary.builder(METRIC_NUM_TRIES) //
                    .tag(TAG_ENTITY, traveler.getEntity()) //
                    .tag(TAG_TENANT, tenantId) //
                    .register(rootRegistry) //
                    .record(numTries);
            DistributionSummary.builder(METRIC_DISTRIBUTION_RETRY) //
                    .tag(TAG_ENTITY, traveler.getEntity()) //
                    .tag(TAG_TENANT, tenantId) //
                    .register(rootRegistry) //
                    .record(numTries > 1 ? 1.0 : 0.0);
            if (numTries > 1) {
                // retry
                Counter.builder(METRIC_HAVE_RETRY_NUM_TRIES) //
                        .tag(TAG_ENTITY, traveler.getEntity()) //
                        .tag(TAG_TENANT, tenantId) //
                        .register(rootRegistry) //
                        .increment(1);
            }
        }
    }

    @Override
    public void registerLookupCache(Cache<Pair<String, EntityLookupEntry>, String> cache, boolean isAllocateMode) {
        if (cache == null) {
            return;
        }

        monitorCache(cache, METRIC_LOOKUP_CACHE, getMatchMode(), isAllocateMode);
    }

    @Override
    public void registerSeedCache(Cache<Pair<Pair<String, String>, String>, EntityRawSeed> cache) {
        if (cache == null) {
            return;
        }

        monitorCache(cache, METRIC_SEED_CACHE, getMatchMode(), false);
    }

    private <K, V> void monitorCache(@NotNull Cache<K, V> cache, @NotNull String metricName, @NotNull String matchMode,
            boolean isAllocateMode) {
        // use the host registry since cache is related to single instance
        CaffeineCacheMetrics.monitor(registryFactory.getHostLevelRegistry(), cache, metricName, //
                TAG_ALLOCATE_ID_MODE, String.valueOf(isAllocateMode), TAG_MATCH_MODE, matchMode);
    }

    private String getMatchMode() {
        return matchActorSystem.isBatchMode() ? MatchActorSystem.BATCH_MODE : MatchActorSystem.REALTIME_MODE;
    }

    private void recordDynamoDistri(@NotNull String metricName, @NotNull EntityMatchEnvironment env,
            @NotNull String tableName, boolean hasEvent) {
        DistributionSummary.builder(metricName) //
                .tag(TAG_ENV, env.name()) //
                .tag(TAG_DYNAMO_TABLE, tableName) //
                .register(registryFactory.getServiceLevelRegistry()) //
                .record(hasEvent ? 1.0 : 0.0);
    }

    private boolean shouldRecord(String tenantId, @NotNull MatchTraveler traveler) {
        // only record entity match visits
        return OperationalMode.ENTITY_MATCH.equals(traveler.getMatchInput().getOperationalMode())
                && StringUtils.isNotBlank(tenantId);
    }

    private String getTenantId(@NotNull MatchTraveler traveler) {
        Tenant tenant = traveler.getMatchInput().getTenant();
        if (tenant == null || tenant.getId() == null) {
            return null;
        }
        return EntityMatchUtils.newStandardizedTenant(tenant).getId();
    }
}
