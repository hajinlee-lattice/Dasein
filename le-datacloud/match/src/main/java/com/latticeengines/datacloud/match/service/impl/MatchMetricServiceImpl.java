package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.common.exposed.metric.MetricNames.Match.METRIC_ACTOR_VISIT;
import static com.latticeengines.common.exposed.metric.MetricNames.Match.METRIC_DATASOURCE_REQ_BATCH_SIZE;
import static com.latticeengines.common.exposed.metric.MetricNames.Match.METRIC_DNB_MATCH_HISTORY;
import static com.latticeengines.common.exposed.metric.MetricNames.Match.METRIC_MATCH_BULK_SIZE;
import static com.latticeengines.common.exposed.metric.MetricNames.Match.METRIC_MATCH_DURATION;
import static com.latticeengines.common.exposed.metric.MetricNames.Match.METRIC_MATCH_RATE;
import static com.latticeengines.common.exposed.metric.MetricNames.Match.METRIC_MATCH_REQ;
import static com.latticeengines.common.exposed.metric.MetricNames.Match.METRIC_NUM_MATCH_INPUT_FIELDS;
import static com.latticeengines.common.exposed.metric.MetricNames.Match.METRIC_NUM_MATCH_OUTPUT_FIELDS;
import static com.latticeengines.common.exposed.metric.MetricNames.Match.METRIC_PENDING_DATASOURCE_LOOKUP_REQ;
import static com.latticeengines.common.exposed.metric.MetricNames.Match.METRIC_TOTAL_MATCHED_ROWS;
import static com.latticeengines.common.exposed.metric.MetricTags.TAG_TENANT;
import static com.latticeengines.common.exposed.metric.MetricTags.Match.TAG_ACTOR;
import static com.latticeengines.common.exposed.metric.MetricTags.Match.TAG_DATACLOUD_VERSION;
import static com.latticeengines.common.exposed.metric.MetricTags.Match.TAG_DNB_CONFIDENCE_CODE;
import static com.latticeengines.common.exposed.metric.MetricTags.Match.TAG_DNB_HIT_BLACK_CACHE;
import static com.latticeengines.common.exposed.metric.MetricTags.Match.TAG_DNB_HIT_WHITE_CACHE;
import static com.latticeengines.common.exposed.metric.MetricTags.Match.TAG_DNB_MATCH_GRADE;
import static com.latticeengines.common.exposed.metric.MetricTags.Match.TAG_DNB_MATCH_STRATEGY;
import static com.latticeengines.common.exposed.metric.MetricTags.Match.TAG_HAS_ERROR;
import static com.latticeengines.common.exposed.metric.MetricTags.Match.TAG_MATCH_MODE;
import static com.latticeengines.common.exposed.metric.MetricTags.Match.TAG_OPERATIONAL_MODE;
import static com.latticeengines.common.exposed.metric.MetricTags.Match.TAG_PREDEFINED_SELECTION;
import static com.latticeengines.common.exposed.metric.MetricTags.Match.TAG_REJECTED;
import static com.latticeengines.common.exposed.metric.MetricTags.Match.TAG_SERVICE_NAME;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.defaultIfEmpty;

import java.time.Duration;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.actors.visitor.impl.DataSourceLookupServiceBase;
import com.latticeengines.datacloud.match.metric.DnBMatchHistory;
import com.latticeengines.datacloud.match.metric.FuzzyMatchHistory;
import com.latticeengines.datacloud.match.service.MatchMetricService;
import com.latticeengines.domain.exposed.actors.VisitingHistory;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatistics;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.monitor.exposed.service.MeterRegistryFactoryService;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;

@Lazy
@Component("matchMetricService")
public class MatchMetricServiceImpl implements MatchMetricService {

    private static final String EMPTY_TAG_VALUE = "__EMPTY__";

    @Lazy
    @Inject
    private MeterRegistryFactoryService registryFactory;

    @Override
    public void registerDataSourceLookupService(DataSourceLookupServiceBase service, boolean isBatchMode) {
        if (service == null) {
            return;
        }

        Gauge.builder(METRIC_PENDING_DATASOURCE_LOOKUP_REQ, () -> {
            Map<String, Integer> stats = service.getTotalPendingReqStats();
            if (MapUtils.isEmpty(stats)) {
                return 0;
            }

            return service.getTotalPendingReqStats().getOrDefault(MatchConstants.REQUEST_NUM, 0);
        }).tag(TAG_SERVICE_NAME, service.getClass().getSimpleName()) //
                .tag(TAG_MATCH_MODE, isBatchMode ? MatchActorSystem.BATCH_MODE : MatchActorSystem.REALTIME_MODE) //
                .register(registryFactory.getHostLevelRegistry(MetricDB.LDC_Match));
    }

    @Override
    public void recordBatchRequestSize(String serviceName, boolean isBatchMode, int size) {
        if (StringUtils.isBlank(serviceName)) {
            return;
        }

        DistributionSummary.builder(METRIC_DATASOURCE_REQ_BATCH_SIZE) //
                .tag(TAG_SERVICE_NAME, serviceName) //
                .tag(TAG_MATCH_MODE, isBatchMode ? MatchActorSystem.BATCH_MODE : MatchActorSystem.REALTIME_MODE) //
                .register(registryFactory.getHostLevelRegistry(MetricDB.LDC_Match))
                .record(size);
    }

    @Override
    public void recordActorVisit(VisitingHistory history) {
        if (history == null || history.getDuration() == null) {
            return;
        }

        Timer.builder(METRIC_ACTOR_VISIT) //
                .tag(TAG_ACTOR, defaultIfBlank(history.getSite(), EMPTY_TAG_VALUE)) //
                .tag(TAG_REJECTED, String.valueOf(Boolean.TRUE.equals(history.getRejected()))) //
                .tag(TAG_MATCH_MODE, defaultIfBlank(history.getActorSystemMode(), EMPTY_TAG_VALUE)) //
                .register(registryFactory.getHostLevelRegistry(MetricDB.LDC_Match)) //
                .record(Duration.ofMillis(history.getDuration()));
    }

    @Override
    public void recordDnBMatch(DnBMatchHistory history) {
        if (history == null || history.getFact() == null || history.getFact().getDuration() == null) {
            return;
        }

        DnBMatchContext ctx = history.getFact();
        Timer.builder(METRIC_DNB_MATCH_HISTORY) //
                .tag(TAG_DNB_CONFIDENCE_CODE,
                        ctx.getConfidenceCode() == null ? EMPTY_TAG_VALUE : String.valueOf(ctx.getConfidenceCode())) //
                .tag(TAG_DNB_MATCH_GRADE,
                        ctx.getMatchGrade() == null ? EMPTY_TAG_VALUE
                                : defaultIfEmpty(ctx.getMatchGrade().getRawCode(), EMPTY_TAG_VALUE)) //
                .tag(TAG_DNB_HIT_BLACK_CACHE, String.valueOf(Boolean.TRUE.equals(ctx.getHitBlackCache()))) //
                .tag(TAG_DNB_HIT_WHITE_CACHE, String.valueOf(Boolean.TRUE.equals(ctx.getHitWhiteCache()))) //
                .tag(TAG_DNB_MATCH_STRATEGY,
                        ctx.getMatchStrategy() == null ? EMPTY_TAG_VALUE
                                : defaultIfEmpty(ctx.getMatchStrategyName(), EMPTY_TAG_VALUE)) //
                .register(registryFactory.getHostLevelRegistry(MetricDB.LDC_Match)) //
                .record(Duration.ofMillis(ctx.getDuration()));
        // TODO add match grade for each match fields
    }

    @Override
    public void recordMatchFinished(MatchContext context) {
        if (context == null || context.getOutput() == null || context.getOutput().getStatistics() == null) {
            return;
        }

        MatchStatistics stats = context.getOutput().getStatistics();
        String tenantId = defaultIfEmpty(getTenantId(context), EMPTY_TAG_VALUE);
        String predefinedSelection = defaultIfEmpty(getPredefinedSelection(context), EMPTY_TAG_VALUE);
        String matchEngine = defaultIfEmpty(getMatchEngine(context), EMPTY_TAG_VALUE);

        if (stats.getTimeElapsedInMsec() != null) {
            getTimer(METRIC_MATCH_DURATION, tenantId, predefinedSelection, matchEngine) //
                    .record(Duration.ofMillis(stats.getTimeElapsedInMsec()));
        }
        if (stats.getRowsRequested() != null) {
            getCounter(METRIC_MATCH_REQ, tenantId, predefinedSelection, matchEngine) //
                    .increment(stats.getRowsRequested());
        }
        if (stats.getRowsMatched() != null) {
            getCounter(METRIC_TOTAL_MATCHED_ROWS, tenantId, predefinedSelection, matchEngine) //
                    .increment(stats.getRowsMatched());
        }
        if (context.getInput() != null && context.getInput().getNumInputFields() != null) {
            getCounter(METRIC_NUM_MATCH_INPUT_FIELDS, tenantId, predefinedSelection, matchEngine) //
                    .increment(context.getInput().getNumInputFields());
        }
        if (context.getOutput() != null && context.getOutput().numOutputFields() != null) {
            getCounter(METRIC_NUM_MATCH_OUTPUT_FIELDS, tenantId, predefinedSelection, matchEngine) //
                    .increment(context.getOutput().numOutputFields());
        }
    }

    @Override
    public void recordBulkSize(BulkMatchOutput bulkMatchOutput) {
        if (bulkMatchOutput == null || bulkMatchOutput.getRowsRequested() == null) {
            return;
        }

        Timer.builder(METRIC_MATCH_BULK_SIZE) //
                .tag(TAG_MATCH_MODE, defaultIfEmpty(bulkMatchOutput.getMatchEngine(), EMPTY_TAG_VALUE)) //
                .register(registryFactory.getHostLevelRegistry(MetricDB.LDC_Match)) //
                .record(Duration.ofMillis(bulkMatchOutput.getTimeElapsed()));
    }

    @Override
    public void recordFuzzyMatchRecord(FuzzyMatchHistory history) {
        if (history == null || history.getFact() == null) {
            return;
        }

        MatchTraveler traveler = history.getFact();

        String hasInvalidValue = String.valueOf(Boolean.TRUE.equals(traveler.getHasInvalidValue()));
        String tenantId = defaultIfEmpty(getTenantId(traveler), EMPTY_TAG_VALUE);
        String dataCloudVersion = defaultIfEmpty(getDataCloudVersion(traveler), EMPTY_TAG_VALUE);
        String operationalMode = defaultIfEmpty(getOperationalMode(traveler), EMPTY_TAG_VALUE);

        // TODO add more tags
        DistributionSummary.builder(METRIC_MATCH_RATE) //
                .tag(TAG_DATACLOUD_VERSION, dataCloudVersion) //
                .tag(TAG_MATCH_MODE, defaultIfEmpty(traveler.getMode(), EMPTY_TAG_VALUE)) //
                .tag(TAG_OPERATIONAL_MODE, operationalMode) //
                .tag(TAG_TENANT, tenantId) //
                .tag(TAG_HAS_ERROR, hasInvalidValue) //
                .register(registryFactory.getServiceLevelRegistry()) //
                .record(Boolean.TRUE.equals(traveler.isMatched()) ? 1.0 : 0.0);

        // TODO add other metrics
    }

    private Timer getTimer(@NotNull String metricName, @NotNull String tenantId, @NotNull String predefinedSelection,
            @NotNull String matchEngine) {
        return Timer.builder(metricName) //
                .tag(TAG_TENANT, tenantId) //
                .tag(TAG_PREDEFINED_SELECTION, predefinedSelection) //
                .tag(TAG_MATCH_MODE, matchEngine) //
                .register(registryFactory.getHostLevelRegistry(MetricDB.LDC_Match));
    }

    private Counter getCounter(@NotNull String metricName, @NotNull String tenantId,
            @NotNull String predefinedSelection, @NotNull String matchEngine) {
        return Counter.builder(metricName) //
                .tag(TAG_TENANT, tenantId) //
                .tag(TAG_PREDEFINED_SELECTION, predefinedSelection) //
                .tag(TAG_MATCH_MODE, matchEngine) //
                .register(registryFactory.getHostLevelRegistry(MetricDB.LDC_Match));
    }

    private String getOperationalMode(@NotNull MatchTraveler traveler) {
        if (traveler.getMatchInput() == null) {
            return null;
        }

        OperationalMode operationalMode = traveler.getMatchInput().getOperationalMode();
        return operationalMode == null ? null : operationalMode.name();
    }

    private String getDataCloudVersion(@NotNull MatchTraveler traveler) {
        if (traveler.getMatchInput() == null) {
            return null;
        }

        return traveler.getMatchInput().getDataCloudVersion();
    }

    private String getMatchEngine(@NotNull MatchContext context) {
        return context.getMatchEngine() == null ? null : context.getMatchEngine().name();
    }

    private String getPredefinedSelection(@NotNull MatchContext context) {
        if (context.getInput() == null || context.getInput().getPredefinedSelection() == null) {
            return null;
        }

        return context.getInput().getPredefinedSelection().getName();
    }

    private String getTenantId(@NotNull MatchContext context) {
        if (context.getInput() == null || context.getInput().getTenant() == null) {
            return null;
        }

        return CustomerSpace.shortenCustomerSpace(context.getInput().getTenant().getId());
    }

    private String getTenantId(@NotNull MatchTraveler traveler) {
        Tenant tenant = traveler.getMatchInput().getTenant();
        if (tenant == null || tenant.getId() == null) {
            return null;
        }
        return CustomerSpace.shortenCustomerSpace(tenant.getId());
    }
}
