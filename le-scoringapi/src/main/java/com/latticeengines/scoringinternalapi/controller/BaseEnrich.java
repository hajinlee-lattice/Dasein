package com.latticeengines.scoringinternalapi.controller;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.common.exposed.rest.RequestIdUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.LogContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.scoringapi.EnrichRequest;
import com.latticeengines.domain.exposed.scoringapi.EnrichResponse;
import com.latticeengines.scoringapi.enrich.EnrichRequestProcessor;
import com.latticeengines.scoringapi.exposed.context.EnrichRequestMetrics;
import com.latticeengines.scoringapi.exposed.context.RequestInfo;
import com.latticeengines.scoringapi.exposed.context.SingleEnrichRecordMeasurement;

public abstract class BaseEnrich extends CommonBase {

    private static final Logger log = LoggerFactory.getLogger(BaseEnrich.class);

    private static final String CREDENTIAL_ID = "credentialId";

    @Autowired
    private EnrichRequestProcessor enrichRequestProcessor;

    protected EnrichResponse enrichRecord(HttpServletRequest request, EnrichRequest enrichRequest,
            CustomerSpace customerSpace, boolean enrichInternalAttributes, String credentialId) {
        requestInfo.put(RequestInfo.TENANT, customerSpace.toString());
        requestInfo.put(CREDENTIAL_ID, credentialId);
        try (LogContext context = new LogContext(MDC_CUSTOMERSPACE, customerSpace)) {
            httpStopWatch.split("parseUuid");
            if (log.isInfoEnabled()) {
                log.info(JsonUtils.serialize(enrichRequest));
            }
            String requestId = RequestIdUtils.getRequestIdentifierId(request);
            EnrichResponse response = enrichRequestProcessor.process(customerSpace, enrichRequest,
                    enrichInternalAttributes, requestId);
            if (warnings.hasWarnings(requestId)) {
                response.setWarnings(warnings.getWarnings(requestId));
                requestInfo.put(WARNINGS, JsonUtils.serialize(warnings.getWarnings(requestId)));
            }
            if (log.isInfoEnabled()) {
                log.info(JsonUtils.serialize(response));
            }

            requestInfo.put(HAS_WARNING, String.valueOf(warnings.hasWarnings(requestId)));
            requestInfo.put(HAS_ERROR, Boolean.toString(false));
            requestInfo.put(IS_BULK_REQUEST, Boolean.FALSE.toString());
            requestInfo.put(IS_ENRICHMENT_REQUESTED, Boolean.toString(true));

            requestInfo.logSummary(requestInfo.getStopWatchSplits());

            EnrichRequestMetrics metrics = generateMetrics(enrichRequest, response, customerSpace, requestId);
            SingleEnrichRecordMeasurement measurement = new SingleEnrichRecordMeasurement(metrics);
            metricService.write(MetricDB.SCORING, measurement);

            return response;
        }
    }

    private EnrichRequestMetrics generateMetrics(EnrichRequest scoreRequest, EnrichResponse response,
            CustomerSpace customerSpace, String requestId) {
        EnrichRequestMetrics metrics = new EnrichRequestMetrics();
        metrics.setHasWarning(warnings.hasWarnings(requestId));
        metrics.setSource(StringUtils.trimToEmpty(scoreRequest.getSource()));
        metrics.setTenantId(customerSpace.toString());
        metrics.setIsEnrich(true);

        Map<String, String> splits = httpStopWatch.getSplits();
        metrics.setParseUuidDurationMS(getSplit(splits, "parseUuidDurationMS"));
        metrics.setMatchRecordDurationMS(getSplit(splits, MATCH_RECORD_DURATION_MS));
        metrics.setRequestDurationMS(getSplit(splits, REQUEST_DURATION_MS));
        metrics.setRequestPreparationDurationMS(getSplit(splits, REQUEST_PREPARATION_DURATION_MS));

        return metrics;
    }

}
