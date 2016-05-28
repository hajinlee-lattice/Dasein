package com.latticeengines.scoringinternalapi.controller;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;

import com.latticeengines.common.exposed.rest.HttpStopWatch;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.LogContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.DebugScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.Fields;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.scoringapi.ModelDetail;
import com.latticeengines.domain.exposed.scoringapi.ModelType;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.Warnings;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.scoringapi.exposed.context.RequestInfo;
import com.latticeengines.scoringapi.exposed.context.RequestMetrics;
import com.latticeengines.scoringapi.exposed.context.SingleRecordMeasurement;
import com.latticeengines.scoringapi.exposed.model.ModelRetriever;
import com.latticeengines.scoringapi.score.ScoreRequestProcessor;

public abstract class BaseScoring {

    protected static final int MAX_ALLOWED_RECORDS = 200;

    private static final String MDC_CUSTOMERSPACE = "customerspace";

    private static final Log log = LogFactory.getLog(BaseScoring.class);

    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ssZ";

    private static final String UTC = "UTC";

    @Autowired
    private HttpStopWatch httpStopWatch;

    @Autowired
    private MetricService metricService;

    @Autowired
    private ModelRetriever modelRetriever;

    @Autowired
    private ScoreRequestProcessor scoreRequestProcessor;

    @Autowired
    private RequestInfo requestInfo;

    @Autowired
    private Warnings warnings;

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);

    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone(UTC));
    }

    protected List<Model> getActiveModels(HttpServletRequest request, ModelType type, CustomerSpace customerSpace) {
        try (LogContext context = new LogContext(MDC_CUSTOMERSPACE, customerSpace)) {
            log.info(type);
            List<Model> models = modelRetriever.getActiveModels(customerSpace, type);
            log.info(JsonUtils.serialize(models));
            return models;
        }
    }

    protected Fields getModelFields(HttpServletRequest request, @PathVariable String modelId,
            CustomerSpace customerSpace) {
        try (LogContext context = new LogContext(MDC_CUSTOMERSPACE, customerSpace)) {
            log.info(modelId);
            Fields fields = modelRetriever.getModelFields(customerSpace, modelId);
            log.info(JsonUtils.serialize(fields));
            return fields;
        }
    }

    protected List<ModelDetail> getPaginatedModels(HttpServletRequest request, String start, int offset, int maximum,
            boolean considerAllStatus, CustomerSpace customerSpace) throws ParseException {
        if (!StringUtils.isEmpty(start)) {
            Date startDate = dateFormat.parse(start);
        }
        return fetchPaginatedModels(request, start, offset, maximum, considerAllStatus, customerSpace);
    }

    protected int getModelCount(HttpServletRequest request, String start, boolean considerAllStatus,
            CustomerSpace customerSpace) throws ParseException {
        if (!StringUtils.isEmpty(start)) {
            Date startDate = dateFormat.parse(start);
        }
        return fetchModelCount(request, start, considerAllStatus, customerSpace);
    }

    protected ScoreResponse scorePercentileRecord(HttpServletRequest request, ScoreRequest scoreRequest,
            CustomerSpace customerSpace) {
        return scoreRecord(request, scoreRequest, false, customerSpace);
    }

    protected List<RecordScoreResponse> scorePercentileRecords(HttpServletRequest request,
            BulkRecordScoreRequest scoreRequest, CustomerSpace customerSpace) {
        return scoreRecords(request, scoreRequest, false, customerSpace);
    }

    protected DebugScoreResponse scoreProbabilityRecord(HttpServletRequest request, ScoreRequest scoreRequest,
            CustomerSpace customerSpace) {
        return (DebugScoreResponse) scoreRecord(request, scoreRequest, true, customerSpace);
    }

    private ScoreResponse scoreRecord(HttpServletRequest request, ScoreRequest scoreRequest, boolean isDebug,
            CustomerSpace customerSpace) {
        requestInfo.put(RequestInfo.TENANT, customerSpace.toString());
        try (LogContext context = new LogContext(MDC_CUSTOMERSPACE, customerSpace)) {
            httpStopWatch.split("getTenantFromOAuth");
            if (log.isInfoEnabled()) {
                log.info(JsonUtils.serialize(scoreRequest));
            }
            ScoreResponse response = scoreRequestProcessor.process(customerSpace, scoreRequest, isDebug);
            if (warnings.hasWarnings()) {
                response.setWarnings(warnings.getWarnings());
                requestInfo.put("Warnings", JsonUtils.serialize(warnings.getWarnings()));
            }
            if (log.isInfoEnabled()) {
                log.info(JsonUtils.serialize(response));
            }

            requestInfo.put("HasWarning", String.valueOf(warnings.hasWarnings()));
            requestInfo.put("HasError", Boolean.toString(false));
            requestInfo.put("Score", String.valueOf(response.getScore()));

            requestInfo.logSummary();

            RequestMetrics metrics = generateMetrics(scoreRequest, response, customerSpace);
            SingleRecordMeasurement measurement = new SingleRecordMeasurement(metrics);
            metricService.write(MetricDB.SCORING, measurement);

            return response;
        }
    }

    private RequestMetrics generateMetrics(ScoreRequest scoreRequest, ScoreResponse response,
            CustomerSpace customerSpace) {
        RequestMetrics metrics = new RequestMetrics();
        metrics.setHasWarning(warnings.hasWarnings());
        metrics.setScore((int) response.getScore());
        metrics.setSource(StringUtils.trimToEmpty(scoreRequest.getSource()));
        metrics.setRule(StringUtils.trimToEmpty(scoreRequest.getRule()));
        metrics.setTenantId(customerSpace.toString());
        metrics.setModelId(scoreRequest.getModelId());

        Map<String, String> splits = httpStopWatch.getSplits();
        metrics.setGetTenantFromOAuthDurationMS(getSplit(splits, "getTenantFromOAuthDurationMS"));
        metrics.setMatchRecordDurationMS(getSplit(splits, "matchRecordDurationMS"));
        metrics.setParseRecordDurationMS(getSplit(splits, "parseRecordDurationMS"));
        metrics.setRequestDurationMS(getSplit(splits, "requestDurationMS"));
        metrics.setRequestPreparationDurationMS(getSplit(splits, "requestPreparationDurationMS"));
        metrics.setRetrieveModelArtifactsDurationMS(getSplit(splits, "retrieveModelArtifactsDurationMS"));
        metrics.setScoreRecordDurationMS(getSplit(splits, "scoreRecordDurationMS"));
        metrics.setTransformRecordDurationMS(getSplit(splits, "transformRecordDurationMS"));

        return metrics;
    }

    private int getSplit(Map<String, String> splits, String key) {
        return Integer.valueOf(splits.get(key));
    }

    private List<ModelDetail> fetchPaginatedModels(HttpServletRequest request, String start, int offset, int maximum,
            boolean considerAllStatus, CustomerSpace customerSpace) {
        try (LogContext context = new LogContext(MDC_CUSTOMERSPACE, customerSpace)) {
            return modelRetriever.getPaginatedModels(customerSpace, start, offset, maximum, considerAllStatus);
        }
    }

    private int fetchModelCount(HttpServletRequest request, String start, boolean considerAllStatus,
            CustomerSpace customerSpace) {
        try (LogContext context = new LogContext(MDC_CUSTOMERSPACE, customerSpace)) {
            return modelRetriever.getModelsCount(customerSpace, start, considerAllStatus);
        }
    }

    private List<RecordScoreResponse> scoreRecords(HttpServletRequest request, BulkRecordScoreRequest scoreRequests,
            boolean isDebug, CustomerSpace customerSpace) {
        if (scoreRequests.getRecords().size() > MAX_ALLOWED_RECORDS) {
            throw new LedpException(LedpCode.LEDP_20027, //
                    new String[] { //
                            new Integer(MAX_ALLOWED_RECORDS).toString(),
                            new Integer(scoreRequests.getRecords().size()).toString() });
        }

        try (LogContext context = new LogContext(MDC_CUSTOMERSPACE, customerSpace)) {
            httpStopWatch.split("getTenantFromOAuth");
            if (log.isInfoEnabled()) {
                log.info(JsonUtils.serialize(scoreRequests));
            }
            List<RecordScoreResponse> response = scoreRequestProcessor.process(customerSpace, scoreRequests, isDebug);

            if (log.isInfoEnabled()) {
                log.info(JsonUtils.serialize(response));
            }

            return response;
        }
    }
}