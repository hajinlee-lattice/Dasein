package com.latticeengines.datacloud.match.service.impl;

import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.visitor.impl.DataSourceLookupServiceBase;
import com.latticeengines.datacloud.match.service.MatchMetricService;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

@Lazy
@Component("matchMetricService")
public class MatchMetricServiceImpl implements MatchMetricService {

    /*
     * metric names
     */
    private static final String METRIC_PENDING_DATASOURCE_LOOKUP_REQ = "match.datasource.req.pending.count";
    private static final String METRIC_DATASOURCE_REQ_BATCH_SIZE = "match.datasource.req.batch.size.dist";

    /*
     * tag names (TODO probably move to a unified constant class)
     */
    private static final String TAG_SERVICE_NAME = "Service";
    private static final String TAG_MATCH_MODE = "MatchMode";

    @Lazy
    @Inject
    @Qualifier("rootHostRegistry")
    private MeterRegistry rootHostRegistry;

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
                .register(rootHostRegistry);
    }

    @Override
    public void recordBatchRequestSize(String serviceName, boolean isBatchMode, int size) {
        if (StringUtils.isBlank(serviceName)) {
            return;
        }

        DistributionSummary.builder(METRIC_DATASOURCE_REQ_BATCH_SIZE) //
                .tag(TAG_SERVICE_NAME, serviceName) //
                .tag(TAG_MATCH_MODE, isBatchMode ? MatchActorSystem.BATCH_MODE : MatchActorSystem.REALTIME_MODE) //
                .register(rootHostRegistry)
                .record(size);
    }
}
