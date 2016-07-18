package com.latticeengines.scoringinternalapi.controller;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;
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
import com.latticeengines.domain.exposed.scoringapi.Record;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse.ScoreModelTuple;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.Warning;
import com.latticeengines.domain.exposed.scoringapi.Warnings;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.scoringapi.exposed.context.RequestInfo;
import com.latticeengines.scoringapi.exposed.context.RequestMetrics;
import com.latticeengines.scoringapi.exposed.context.SingleRecordMeasurement;
import com.latticeengines.scoringapi.exposed.model.ModelRetriever;
import com.latticeengines.scoringapi.score.ScoreRequestProcessor;

public abstract class BaseScoring {

    private static final String REQUEST_DURATION_MS = "requestDurationMS";

    private static final String SOURCE = "Source";

    private static final String TOTAL_RECORDS = "TotalRecords";

    private static final String IS_BULK_REQUEST = "IsBulkRequest";

    private static final String IS_ENRICHMENT_REQUESTED = "IsEnrichmentRequested";

    private static final String ID_TYPE = "IdType";

    private static final String RULE = "Rule";

    private static final String MODEL_ID = "ModelId";

    private static final String LATTICE_ID = "LatticeId";

    private static final String RECORD_ID = "RecordId";

    private static final String SCORE = "Score";

    private static final String ERROR_KEY = "Error";

    private static final String ERRORS = "errors";

    private static final String ERROR_DESCRIPTION = "error_description";

    private static final String ERROR = "error";

    private static final String HAS_WARNING = "HasWarning";

    private static final String HAS_ERROR = "HasError";

    private static final String WARNINGS = "Warnings";

    private static final String RECORD_CARDINALITY = "RecordCardinality";

    private static final String GET_TENANT_FROM_OAUTH = "getTenantFromOAuth";

    private static final String AVERAGE_TOTAL_DURATION_PER_RECORD = "requestDurationAveragePerRecordMS";

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

    public static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);

    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone(UTC));
    }

    protected List<Model> getActiveModels(HttpServletRequest request, ModelType type,
            CustomerSpace customerSpace) {
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

    protected List<ModelDetail> getPaginatedModels(HttpServletRequest request, String start,
            int offset, int maximum, boolean considerAllStatus, CustomerSpace customerSpace)
            throws ParseException {
        start = validateStartValue(start);
        return fetchPaginatedModels(request, start, offset, maximum, considerAllStatus,
                customerSpace);
    }

    protected int getModelCount(HttpServletRequest request, String start, boolean considerAllStatus,
            CustomerSpace customerSpace) throws ParseException {
        start = validateStartValue(start);
        return fetchModelCount(request, start, considerAllStatus, customerSpace);
    }

    protected ScoreResponse scorePercentileRecord(HttpServletRequest request,
            ScoreRequest scoreRequest, CustomerSpace customerSpace) {
        return scoreRecord(request, scoreRequest, false, customerSpace);
    }

    protected List<RecordScoreResponse> scorePercentileRecords(HttpServletRequest request,
            BulkRecordScoreRequest scoreRequest, CustomerSpace customerSpace) {
        return scoreRecords(request, scoreRequest, false, customerSpace);
    }

    protected List<RecordScoreResponse> scoreRecordsDebug(HttpServletRequest request,
            BulkRecordScoreRequest scoreRequest, CustomerSpace customerSpace) {
        return scoreRecords(request, scoreRequest, true, customerSpace);
    }

    protected DebugScoreResponse scoreProbabilityRecord(HttpServletRequest request,
            ScoreRequest scoreRequest, CustomerSpace customerSpace) {
        return (DebugScoreResponse) scoreRecord(request, scoreRequest, true, customerSpace);
    }

    private ScoreResponse scoreRecord(HttpServletRequest request, ScoreRequest scoreRequest,
            boolean isDebug, CustomerSpace customerSpace) {
        requestInfo.put(RequestInfo.TENANT, customerSpace.toString());
        try (LogContext context = new LogContext(MDC_CUSTOMERSPACE, customerSpace)) {
            httpStopWatch.split(GET_TENANT_FROM_OAUTH);
            if (log.isInfoEnabled()) {
                log.info(JsonUtils.serialize(scoreRequest));
            }
            ScoreResponse response = scoreRequestProcessor.process(customerSpace, scoreRequest,
                    isDebug);
            if (warnings.hasWarnings()) {
                response.setWarnings(warnings.getWarnings());
                requestInfo.put(WARNINGS, JsonUtils.serialize(warnings.getWarnings()));
            }
            if (log.isInfoEnabled()) {
                log.info(JsonUtils.serialize(response));
            }

            requestInfo.put(HAS_WARNING, String.valueOf(warnings.hasWarnings()));
            requestInfo.put(HAS_ERROR, Boolean.toString(false));
            requestInfo.put(SCORE, String.valueOf(response.getScore()));
            requestInfo.put(IS_BULK_REQUEST, Boolean.FALSE.toString());
            requestInfo.put(IS_ENRICHMENT_REQUESTED,
                    String.valueOf(scoreRequest.isPerformEnrichment()));
            requestInfo.put(RECORD_ID, response.getId());
            requestInfo.put(LATTICE_ID, response.getLatticeId());
            requestInfo.put(ID_TYPE, scoreRequest.getIdType());

            requestInfo.logSummary(requestInfo.getStopWatchSplits());

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
        metrics.setRequestDurationMS(getSplit(splits, REQUEST_DURATION_MS));
        metrics.setRequestPreparationDurationMS(getSplit(splits, "requestPreparationDurationMS"));
        metrics.setRetrieveModelArtifactsDurationMS(
                getSplit(splits, "retrieveModelArtifactsDurationMS"));
        metrics.setScoreRecordDurationMS(getSplit(splits, "scoreRecordDurationMS"));
        metrics.setTransformRecordDurationMS(getSplit(splits, "transformRecordDurationMS"));

        return metrics;
    }

    private int getSplit(Map<String, String> splits, String key) {
        return Integer.valueOf(splits.get(key));
    }

    private List<ModelDetail> fetchPaginatedModels(HttpServletRequest request, String start,
            int offset, int maximum, boolean considerAllStatus, CustomerSpace customerSpace) {
        try (LogContext context = new LogContext(MDC_CUSTOMERSPACE, customerSpace)) {
            return modelRetriever.getPaginatedModels(customerSpace, start, offset, maximum,
                    considerAllStatus);
        }
    }

    private int fetchModelCount(HttpServletRequest request, String start, boolean considerAllStatus,
            CustomerSpace customerSpace) {
        try (LogContext context = new LogContext(MDC_CUSTOMERSPACE, customerSpace)) {
            return modelRetriever.getModelsCount(customerSpace, start, considerAllStatus);
        }
    }

    private List<RecordScoreResponse> scoreRecords(HttpServletRequest request,
            BulkRecordScoreRequest scoreRequests, boolean isDebug, CustomerSpace customerSpace) {
        if (scoreRequests.getRecords().size() > MAX_ALLOWED_RECORDS) {
            throw new LedpException(LedpCode.LEDP_20027, //
                    new String[] { //
                            new Integer(MAX_ALLOWED_RECORDS).toString(),
                            new Integer(scoreRequests.getRecords().size()).toString() });
        }

        requestInfo.put(RequestInfo.TENANT, customerSpace.toString());
        List<RecordScoreResponse> response = null;

        try (LogContext context = new LogContext(MDC_CUSTOMERSPACE, customerSpace)) {
            httpStopWatch.split(GET_TENANT_FROM_OAUTH);
            if (log.isInfoEnabled()) {
                log.info(JsonUtils.serialize(scoreRequests));
            }
            response = scoreRequestProcessor.process(customerSpace, scoreRequests, isDebug);

            if (log.isInfoEnabled()) {
                log.info(JsonUtils.serialize(response));
            }

            return response;
        } finally {
            logBulkScoreSummary(scoreRequests, response);
        }
    }

    private void logBulkScoreSummary(BulkRecordScoreRequest scoreRequests,
            List<RecordScoreResponse> responseList) {
        try {
            requestInfo.put(IS_BULK_REQUEST, Boolean.TRUE.toString());
            requestInfo.put(SOURCE, scoreRequests.getSource());
            Map<String, String> stopWatchSplits = requestInfo.getStopWatchSplits();

            if (CollectionUtils.isEmpty(responseList)) {
                requestInfo.logSummary(stopWatchSplits);
            } else {
                int idx = 0;
                requestInfo.put(TOTAL_RECORDS, String.valueOf(responseList.size()));
                if (stopWatchSplits.get(REQUEST_DURATION_MS) != null) {
                    try {
                        int avgTime = new Float(
                                (Integer.parseInt(stopWatchSplits.get(REQUEST_DURATION_MS)) * 1.0
                                        / responseList.size())).intValue();
                        requestInfo.put(AVERAGE_TOTAL_DURATION_PER_RECORD, String.valueOf(avgTime));
                    } catch (Exception ex) {
                        // ignore any exception as it should not fail overall
                        // score
                        // request
                    }
                }
                for (RecordScoreResponse resp : responseList) {
                    Record record = scoreRequests.getRecords().get(idx++);
                    for (ScoreModelTuple scoreTuple : resp.getScores()) {
                        Map<String, String> logMap = new HashMap<>();
                        String modelId = scoreTuple.getModelId();
                        Double score = scoreTuple.getScore();
                        String error = scoreTuple.getError();
                        String errorDesc = scoreTuple.getErrorDescription();
                        List<Warning> warningList = new ArrayList<>();
                        requestInfo.put(WARNINGS, null);
                        if (!CollectionUtils.isEmpty(resp.getWarnings())) {
                            for (Warning warning : warnings.getWarnings()) {
                                if (warning.getDescription().contains(modelId)) {
                                    warningList.add(warning);
                                }
                            }

                            if (!warningList.isEmpty()) {
                                requestInfo.put(WARNINGS, JsonUtils.serialize(warningList));
                            }
                        }

                        requestInfo.put(HAS_WARNING, String.valueOf(!warningList.isEmpty()));
                        boolean hasError = !com.latticeengines.common.exposed.util.StringUtils
                                .objectIsNullOrEmptyString(error);
                        requestInfo.put(HAS_ERROR, Boolean.toString(hasError));

                        if (hasError) {
                            Map<String, String> errorMap = new HashMap<>();
                            errorMap.put(ERROR, error);
                            errorMap.put(ERROR_DESCRIPTION, errorDesc);
                            errorMap.put(ERRORS, JsonUtils.serialize(new ArrayList<String>()));

                            requestInfo.put(ERROR_KEY, JsonUtils.serialize(errorMap));
                        } else {
                            requestInfo.put(ERROR_KEY, null);
                        }
                        requestInfo.put(SCORE, String.valueOf(score));
                        requestInfo.put(RECORD_ID, resp.getId());
                        requestInfo.put(RECORD_CARDINALITY,
                                String.valueOf(record.getModelAttributeValuesMap().size()));
                        requestInfo.put(LATTICE_ID, resp.getLatticeId());
                        requestInfo.put(MODEL_ID, scoreTuple.getModelId());
                        requestInfo.put(RULE, record.getRule());
                        requestInfo.put(ID_TYPE, record.getIdType());
                        requestInfo.put(IS_ENRICHMENT_REQUESTED,
                                String.valueOf(record.isPerformEnrichment()));

                        requestInfo.putAll(logMap);
                        requestInfo.logSummary(stopWatchSplits);
                    }
                }
            }
        } catch (Exception ex) {
            log.debug("Any exception in logging block should not fail rest of the scoring: "
                    + ex.getMessage(), ex);
        }
    }

    private String validateStartValue(String start) throws ParseException {
        try {
            Date startDate = null;
            if (!StringUtils.isEmpty(start)) {
                startDate = dateFormat.parse(start);
            } else {
                // if no start date is specified then default start date is
                // start of epoch time 1971-1-1
                startDate = new Date(0);
                start = dateFormat.format(startDate);
            }
            return start;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31106, e, new String[] { start });
        }
    }
}