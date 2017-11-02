package com.latticeengines.scoringinternalapi.controller;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.PathVariable;

import com.latticeengines.common.exposed.util.DateTimeUtils;
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
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.scoringapi.exposed.context.RequestInfo;
import com.latticeengines.scoringapi.exposed.context.ScoreRequestMetrics;
import com.latticeengines.scoringapi.exposed.context.SingleRecordMeasurement;
import com.latticeengines.scoringapi.exposed.model.ModelRetriever;
import com.latticeengines.scoringapi.score.ScoreRequestProcessor;

public abstract class BaseScoring extends CommonBase {

    private static final Logger log = LoggerFactory.getLogger(BaseScoring.class);

    private static final String SOURCE = "Source";

    private static final String TOTAL_RECORDS = "TotalRecords";

    private static final String TOTAL_FAILED_RECORDS = "TotalFailedRecords";

    private static final String ENFORCE_FUZZY_MATCH = "EnforceFuzzyMatch";

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

    private static final String RECORD_CARDINALITY = "RecordCardinality";

    private static final String GET_TENANT_FROM_OAUTH = "getTenantFromOAuth";

    protected static final int MAX_ALLOWED_RECORDS = 200;

    private static final String TOTAL_TIME_PREFIX = "total_";

    @Autowired
    protected OAuthUserEntityMgr oAuthUserEntityMgr;

    @Autowired
    private ModelRetriever modelRetriever;

    @Autowired
    private ScoreRequestProcessor scoreRequestProcessor;

    protected List<Model> getActiveModels(HttpServletRequest request, ModelType type, CustomerSpace customerSpace) {
        try (LogContext context = new LogContext(MDC_CUSTOMERSPACE, customerSpace)) {
            log.info(type.toString());
            List<Model> models = modelRetriever.getActiveModels(customerSpace, type);
            log.info(JsonUtils.serialize(models));
            return models;
        }
    }

    protected Fields getModelFields(HttpServletRequest request, @PathVariable String modelId,
            CustomerSpace customerSpace) {
        try (LogContext context = new LogContext(MDC_CUSTOMERSPACE, customerSpace)) {
            log.info(String.format("Getting model fields for the model %s", modelId));
            Fields fields = modelRetriever.getModelFields(customerSpace, modelId);
            log.info(JsonUtils.serialize(fields));
            return fields;
        }
    }

    protected List<ModelDetail> getPaginatedModels(HttpServletRequest request, String start, int offset, int maximum,
            boolean considerAllStatus, CustomerSpace customerSpace) throws ParseException {
        start = validateStartValue(start);
        return fetchPaginatedModels(request, start, offset, maximum, considerAllStatus, customerSpace);
    }

    protected int getModelCount(HttpServletRequest request, String start, boolean considerAllStatus,
            CustomerSpace customerSpace) throws ParseException {
        start = validateStartValue(start);
        return fetchModelCount(request, start, considerAllStatus, customerSpace);
    }

    protected ScoreResponse scorePercentileRecord(HttpServletRequest request, ScoreRequest scoreRequest,
            CustomerSpace customerSpace, boolean enrichInternalAttributes, boolean performFetchOnlyForMatching,
            String requestId) {
        return scoreRecord(request, scoreRequest, false, customerSpace, enrichInternalAttributes,
                performFetchOnlyForMatching, requestId);
    }

    protected List<RecordScoreResponse> scorePercentileRecords(HttpServletRequest request,
            BulkRecordScoreRequest scoreRequest, CustomerSpace customerSpace, boolean enrichInternalAttributes,
            boolean performFetchOnlyForMatching, String requestId) {
        return scoreRecords(request, scoreRequest, false, customerSpace, enrichInternalAttributes,
                performFetchOnlyForMatching, requestId);
    }

    protected List<RecordScoreResponse> scoreRecordsDebug(HttpServletRequest request,
            BulkRecordScoreRequest scoreRequest, CustomerSpace customerSpace, boolean enrichInternalAttributes,
            boolean performFetchOnlyForMatching, String requestId) {
        return scoreRecords(request, scoreRequest, true, customerSpace, enrichInternalAttributes,
                performFetchOnlyForMatching, requestId);
    }

    protected DebugScoreResponse scoreProbabilityRecord(HttpServletRequest request, ScoreRequest scoreRequest,
            CustomerSpace customerSpace, boolean enrichInternalAttributes, boolean performFetchOnlyForMatching,
            String requestId) {
        return (DebugScoreResponse) scoreRecord(request, scoreRequest, true, customerSpace, enrichInternalAttributes,
                performFetchOnlyForMatching, requestId);
    }

    protected DebugScoreResponse scoreAndEnrichRecordApiConsole(HttpServletRequest request, ScoreRequest scoreRequest,
            CustomerSpace customerSpace, boolean enrichInternalAttributes, String requestId, boolean enforceFuzzyMatch,
            boolean skipDnBCache) {
        return (DebugScoreResponse) scoreRecord(request, scoreRequest, true, customerSpace, enrichInternalAttributes,
                false, requestId, true, enforceFuzzyMatch, skipDnBCache);
    }

    private ScoreResponse scoreRecord(HttpServletRequest request, ScoreRequest scoreRequest, boolean isDebug,
            CustomerSpace customerSpace, boolean enrichInternalAttributes, boolean performFetchOnlyForMatching,
            String requestId) {
        return scoreRecord(request, scoreRequest, isDebug, customerSpace, enrichInternalAttributes,
                performFetchOnlyForMatching, requestId, false, false, false);
    }

    private ScoreResponse scoreRecord(HttpServletRequest request, ScoreRequest scoreRequest, boolean isDebug,
            CustomerSpace customerSpace, boolean enrichInternalAttributes, boolean performFetchOnlyForMatching,
            String requestId, boolean isCalledViaApiConsole, boolean enforceFuzzyMatch, boolean skipDnBCache) {
        requestInfo.put(RequestInfo.TENANT, customerSpace.toString());
        try (LogContext context = new LogContext(MDC_CUSTOMERSPACE, customerSpace)) {
            httpStopWatch.split(GET_TENANT_FROM_OAUTH);
            if (log.isInfoEnabled()) {
                log.info(JsonUtils.serialize(scoreRequest));
            }

            ScoreResponse response = scoreRequestProcessor.process(customerSpace, scoreRequest, isDebug,
                    enrichInternalAttributes, performFetchOnlyForMatching, requestId, isCalledViaApiConsole,
                    enforceFuzzyMatch, skipDnBCache);
            if (warnings.hasWarnings(requestId)) {
                response.setWarnings(warnings.getWarnings(requestId));
                requestInfo.put(WARNINGS, JsonUtils.serialize(warnings.getWarnings(requestId)));
            }
            if (log.isInfoEnabled()) {
                log.info(JsonUtils.serialize(response));
            }

            requestInfo.put(HAS_WARNING, String.valueOf(warnings.hasWarnings(requestId)));
            requestInfo.put(HAS_ERROR, Boolean.toString(false));
            requestInfo.put(SCORE, String.valueOf(response.getScore()));
            requestInfo.put(IS_BULK_REQUEST, Boolean.FALSE.toString());
            requestInfo.put(IS_ENRICHMENT_REQUESTED, String.valueOf(scoreRequest.isPerformEnrichment()));
            requestInfo.put(RECORD_ID, response.getId());
            requestInfo.put(LATTICE_ID, response.getLatticeId());
            requestInfo.put(ID_TYPE, scoreRequest.getIdType());
            requestInfo.put(ENFORCE_FUZZY_MATCH,
                    enforceFuzzyMatch ? Boolean.TRUE.toString() : Boolean.FALSE.toString());

            requestInfo.logSummary(requestInfo.getStopWatchSplits());

            ScoreRequestMetrics metrics = generateMetrics(scoreRequest, response, customerSpace, requestId);
            SingleRecordMeasurement measurement = new SingleRecordMeasurement(metrics);
            metricService.write(MetricDB.SCORING, measurement);

            return response;
        }
    }

    private ScoreRequestMetrics generateMetrics(ScoreRequest scoreRequest, ScoreResponse response,
            CustomerSpace customerSpace, String requestId) {
        ScoreRequestMetrics metrics = new ScoreRequestMetrics();
        metrics.setHasWarning(warnings.hasWarnings(requestId));
        metrics.setScore(response.getScore());
        metrics.setSource(StringUtils.trimToEmpty(scoreRequest.getSource()));
        metrics.setRule(StringUtils.trimToEmpty(scoreRequest.getRule()));
        metrics.setTenantId(customerSpace.toString());
        metrics.setModelId(scoreRequest.getModelId());
        metrics.setIsEnrich(scoreRequest.isPerformEnrichment());

        Map<String, String> splits = httpStopWatch.getSplits();
        metrics.setGetTenantFromOAuthDurationMS(getSplit(splits, "getTenantFromOAuthDurationMS"));
        metrics.setMatchRecordDurationMS(getSplit(splits, MATCH_RECORD_DURATION_MS));
        metrics.setParseRecordDurationMS(getSplit(splits, "parseRecordDurationMS"));
        metrics.setRequestDurationMS(getSplit(splits, REQUEST_DURATION_MS));
        metrics.setRequestPreparationDurationMS(getSplit(splits, REQUEST_PREPARATION_DURATION_MS));
        if (splits.containsKey("retrieveModelArtifactsDurationMS")) {
            metrics.setRetrieveModelArtifactsDurationMS(getSplit(splits, "retrieveModelArtifactsDurationMS"));
        }
        if (splits.containsKey("scoreRecordDurationMS")) {
            metrics.setScoreRecordDurationMS(getSplit(splits, "scoreRecordDurationMS"));
        }
        if (splits.containsKey("transformRecordDurationMS")) {
            metrics.setTransformRecordDurationMS(getSplit(splits, "transformRecordDurationMS"));
        }

        return metrics;
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
            boolean isDebug, CustomerSpace customerSpace, boolean enrichInternalAttributes,
            boolean performFetchOnlyForMatching, String requestId) {
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
            response = scoreRequestProcessor.process(customerSpace, scoreRequests, isDebug, enrichInternalAttributes,
                    performFetchOnlyForMatching, requestId);

            if (log.isInfoEnabled()) {
                log.info(JsonUtils.serialize(response));
            }

            return response;
        } finally {
            logBulkScoreSummary(scoreRequests, response);
        }
    }

    private void logBulkScoreSummary(BulkRecordScoreRequest scoreRequests, List<RecordScoreResponse> responseList) {
        try {
            requestInfo.put(IS_BULK_REQUEST, Boolean.TRUE.toString());
            requestInfo.put(SOURCE, scoreRequests.getSource());
            Map<String, String> stopWatchSplits = requestInfo.getStopWatchSplits();

            if (CollectionUtils.isEmpty(responseList)) {
                requestInfo.logSummary(stopWatchSplits);
            } else {
                requestInfo.put(TOTAL_RECORDS, String.valueOf(responseList.size()));

                // get total number of errors
                Long totalFailedRecords = //
                        responseList.stream() //
                                .flatMap(resp -> resp.getScores().stream()) //
                                .filter(sco -> !com.latticeengines.common.exposed.util.StringStandardizationUtils
                                        .objectIsNullOrEmptyString(sco.getError())) //
                                .count();

                requestInfo.put(TOTAL_FAILED_RECORDS, totalFailedRecords.toString());

                Map<String, String> totalDurationStopWatchSplits = new HashMap<>();

                processDurationMSForLogging(responseList, stopWatchSplits, totalDurationStopWatchSplits);
                logTotalDurationSummary(totalDurationStopWatchSplits);
                requestInfo.remove(TOTAL_FAILED_RECORDS);

                int idx = 0;
                for (RecordScoreResponse resp : responseList) {
                    Record record = scoreRequests.getRecords().get(idx++);
                    for (ScoreModelTuple scoreTuple : resp.getScores()) {
                        Map<String, String> logMap = new HashMap<>();
                        String modelId = scoreTuple.getModelId();
                        Integer score = scoreTuple.getScore();
                        String error = scoreTuple.getError();
                        String errorDesc = scoreTuple.getErrorDescription();
                        String recordId = record.getRecordId();
                        List<Warning> warningList = new ArrayList<>();
                        requestInfo.put(WARNINGS, null);
                        if (!CollectionUtils.isEmpty(resp.getWarnings())) {
                            if (!CollectionUtils.isEmpty(warnings.getWarnings(recordId))) {
                                for (Warning warning : warnings.getWarnings(recordId)) {
                                    warningList.add(warning);
                                }
                            }

                            if (!warningList.isEmpty()) {
                                requestInfo.put(WARNINGS, JsonUtils.serialize(warningList));
                            }
                        }

                        requestInfo.put(HAS_WARNING, String.valueOf(!warningList.isEmpty()));
                        boolean hasError = !com.latticeengines.common.exposed.util.StringStandardizationUtils
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
                        requestInfo.put(RECORD_CARDINALITY, String.valueOf(record.getModelAttributeValuesMap().size()));
                        requestInfo.put(LATTICE_ID, resp.getLatticeId());
                        requestInfo.put(MODEL_ID, scoreTuple.getModelId());
                        requestInfo.put(RULE, record.getRule());
                        requestInfo.put(ID_TYPE, record.getIdType());
                        requestInfo.put(IS_ENRICHMENT_REQUESTED, String.valueOf(record.isPerformEnrichment()));

                        requestInfo.putAll(logMap);
                        requestInfo.logSummary(stopWatchSplits);
                    }
                }
            }
        } catch (Exception ex) {
            log.debug("Any exception in logging block should not fail rest of the scoring: " + ex.getMessage(), ex);
        }
    }

    private void logTotalDurationSummary(Map<String, String> totalDurationStopWatchSplits) {
        requestInfo.logAggregateSummary(totalDurationStopWatchSplits);
        for (String totalDurationKey : totalDurationStopWatchSplits.keySet()) {
            requestInfo.remove(totalDurationKey);
        }
    }

    private void processDurationMSForLogging(List<RecordScoreResponse> responseList,
            Map<String, String> stopWatchSplits, Map<String, String> totalDurationStopWatchSplits) {
        if (stopWatchSplits.get(REQUEST_DURATION_MS) != null) {
            Map<String, String> originalStopWatchSplits = new HashMap<>(stopWatchSplits);

            stopWatchSplits.clear();

            for (String timeKey : originalStopWatchSplits.keySet()) {
                int totalTimeTaken = Integer.parseInt(originalStopWatchSplits.get(timeKey));
                int avgTimeTaken = new Float(totalTimeTaken * 1.0 / responseList.size()).intValue();

                stopWatchSplits.put(timeKey, String.valueOf(avgTimeTaken));
                totalDurationStopWatchSplits.put(TOTAL_TIME_PREFIX + timeKey, String.valueOf(totalTimeTaken));
            }
        }
    }

    private String validateStartValue(String start) throws ParseException {
        try {
            Date startDate = null;
            if (!StringUtils.isEmpty(start)) {
                startDate = DateTimeUtils.convertToDateUTCISO8601(start);
            } else {
                // if no start date is specified then default start date is
                // start of epoch time 1971-1-1
                startDate = new Date(0);
                start = DateTimeUtils.convertToStringUTCISO8601(startDate);
            }
            return start;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31106, e, new String[] { start });
        }
    }
}
