package com.latticeengines.scoringinternalapi.controller;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.common.exposed.rest.HttpStopWatch;
import com.latticeengines.domain.exposed.scoringapi.Warnings;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.scoringapi.exposed.context.RequestInfo;

public abstract class CommonBase {

    protected static final String REQUEST_PREPARATION_DURATION_MS = "requestPreparationDurationMS";

    protected static final String MATCH_RECORD_DURATION_MS = "matchRecordDurationMS";

    protected static final String HAS_WARNING = "HasWarning";

    protected static final String HAS_ERROR = "HasError";

    protected static final String WARNINGS = "Warnings";

    protected static final String IS_BULK_REQUEST = "IsBulkRequest";

    protected static final String IS_ENRICHMENT_REQUESTED = "IsEnrichmentRequested";

    protected static final String MDC_CUSTOMERSPACE = "customerspace";

    protected static final String REQUEST_DURATION_MS = "requestDurationMS";

    @Autowired
    protected HttpStopWatch httpStopWatch;

    @Autowired
    protected MetricService metricService;

    @Autowired
    protected RequestInfo requestInfo;

    @Autowired
    protected Warnings warnings;

    protected int getSplit(Map<String, String> splits, String key) {
        return Integer.valueOf(splits.get(key));
    }

}
