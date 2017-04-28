package com.latticeengines.scoringapi.score.impl;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryProvenance;
import com.latticeengines.domain.exposed.pls.ModelSummaryProvenanceProperty;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.DebugRecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.DebugScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.domain.exposed.scoringapi.Record;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse.ScoreModelTuple;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.Warning;
import com.latticeengines.domain.exposed.scoringapi.WarningCode;
import com.latticeengines.domain.exposed.scoringapi.Warnings;
import com.latticeengines.scoringapi.entitymanager.ScoreHistoryEntityMgr;
import com.latticeengines.scoringapi.exposed.InterpretedFields;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;
import com.latticeengines.scoringapi.exposed.exception.ScoringApiException;
import com.latticeengines.scoringapi.exposed.model.ModelJsonTypeHandler;
import com.latticeengines.scoringapi.exposed.model.ModelRetriever;
import com.latticeengines.scoringapi.match.Matcher;
import com.latticeengines.scoringapi.score.ScoreRequestProcessor;
import com.latticeengines.scoringapi.transform.RecordTransformer;
import com.latticeengines.scoringinternalapi.controller.BaseScoring;

@Component("scoreRequestProcessor")
public class ScoreRequestProcessorImpl extends BaseRequestProcessorImpl implements ScoreRequestProcessor {
    private static final Log log = LogFactory.getLog(ScoreRequestProcessorImpl.class);

    @Autowired
    private ModelRetriever modelRetriever;

    @Autowired
    private RecordTransformer recordTransformer;

    @Autowired
    private Warnings warnings;

    @Autowired
    private List<ModelJsonTypeHandler> modelJsonTypeHandlers;

    @Autowired
    private ScoreHistoryEntityMgr scoreHistoryEntityMgr;

    @Value("${scoringapi.score.history.publish.enabled:false}")
    private boolean shouldPublish;

    @Override
    public ScoreResponse process(CustomerSpace space, ScoreRequest request, boolean isDebug, //
            boolean enrichInternalAttributes, boolean performFetchOnlyForMatching, String requestId,
            boolean isCalledViaApiConsole) {
        return process(space, request, isDebug, enrichInternalAttributes, performFetchOnlyForMatching, requestId,
                isCalledViaApiConsole, false, false);
    }

    @Override
    public ScoreResponse process(CustomerSpace space, ScoreRequest request, boolean isDebug, //
            boolean enrichInternalAttributes, boolean performFetchOnlyForMatching, String requestId,
            boolean isCalledViaApiConsole, boolean enforceFuzzyMatch, boolean skipDnBCache) {
        split("requestPreparation");
        requestInfo.put("Rule", Strings.nullToEmpty(request.getRule()));
        requestInfo.put("Source", Strings.nullToEmpty(request.getSource()));

        ScoringArtifacts scoringArtifacts = null;
        boolean shouldSkipMatching = false;
        Map<String, FieldSchema> fieldSchemas = null;
        ModelJsonTypeHandler modelJsonTypeHandler = null;

        boolean shouldSkipScoring = false;

        if (!isCalledViaApiConsole || !StringStandardizationUtils.objectIsNullOrEmptyString(request.getModelId())) {
            if (org.apache.commons.lang.StringUtils.isBlank(request.getModelId())) {
                throw new ScoringApiException(LedpCode.LEDP_31101);
            }

            scoringArtifacts = modelRetriever.getModelArtifacts(space, request.getModelId());
            requestInfo.put("ModelId", scoringArtifacts.getModelSummary().getId());
            requestInfo.put("ModelName", scoringArtifacts.getModelSummary().getDisplayName());
            if (scoringArtifacts.getModelSummary().getStatus() != ModelSummaryStatus.ACTIVE) {
                throw new ScoringApiException(LedpCode.LEDP_31114,
                        new String[] { scoringArtifacts.getModelSummary().getId() });
            }
            requestInfo.put("ModelType",
                    (scoringArtifacts.getModelType() == null ? "" : scoringArtifacts.getModelType().name()));
            shouldSkipMatching = shouldSkipMatching(scoringArtifacts, performFetchOnlyForMatching);
            fieldSchemas = scoringArtifacts.getFieldSchemas();

            split("retrieveModelArtifacts");

            modelJsonTypeHandler = getModelJsonTypeHandler(scoringArtifacts.getModelJsonType());

            if (!shouldSkipMatching) {
                ScoringApiException missingEssentialFieldsException = checkForMissingFields(scoringArtifacts,
                        fieldSchemas, request.getRecord(), modelJsonTypeHandler);
                if (missingEssentialFieldsException != null) {
                    if (!performFetchOnlyForMatching || StringStandardizationUtils.objectIsNullOrEmptyString(
                            request.getRecord().get(FieldInterpretation.LatticeAccountId.toString()))) {
                        throw missingEssentialFieldsException;
                    }
                }
            }
        } else {
            shouldSkipScoring = true;
            fieldSchemas = new HashMap<>();
            modelJsonTypeHandler = getModelJsonTypeHandler("NOT" + ModelJsonTypeHandler.PMML_MODEL);
        }

        AbstractMap.SimpleEntry<Map<String, Object>, InterpretedFields> parsedRecordAndInterpretedFields = parseRecord(
                fieldSchemas, request.getRecord(), modelJsonTypeHandler);
        String recordId = getIdIfAvailable(parsedRecordAndInterpretedFields.getValue(), request.getRecord());
        requestInfo.put("RecordId", recordId);
        split("parseRecord");

        Map<String, Object> readyToTransformRecord = null;
        Map<String, Object> enrichmentAttributes = null;

        List<String> matchLogs = new ArrayList<>();
        List<String> matchErrorLogs = new ArrayList<>();

        if (shouldSkipMatching) {
            if (request.isPerformEnrichment()) {
                // pass null model summary for performing matching for
                // enrichment only
                ModelSummary modelSummary = null;

                Map<String, Map<String, Object>> matchedRecordEnrichmentMap = getMatcher(false).matchAndJoin(space,
                        parsedRecordAndInterpretedFields.getValue(), fieldSchemas,
                        parsedRecordAndInterpretedFields.getKey(), modelSummary, request.isPerformEnrichment(),
                        enrichInternalAttributes, performFetchOnlyForMatching, requestId, isDebug, matchLogs,
                        matchErrorLogs, isCalledViaApiConsole, enforceFuzzyMatch, skipDnBCache);
                Map<String, Object> matchedRecord = extractMap(matchedRecordEnrichmentMap, Matcher.RESULT);
                addMissingFields(fieldSchemas, matchedRecord);
                readyToTransformRecord = matchedRecord;
                enrichmentAttributes = extractMap(matchedRecordEnrichmentMap, Matcher.ENRICHMENT);
            } else {
                Map<String, Object> formattedRecord = parsedRecordAndInterpretedFields.getKey();
                addMissingFields(fieldSchemas, formattedRecord);
                readyToTransformRecord = formattedRecord;
            }
        } else {
            ModelSummary modelSummary = null;
            if (!shouldSkipScoring) {
                modelSummary = scoringArtifacts.getModelSummary();
            }

            Map<String, Map<String, Object>> matchedRecordEnrichmentMap = getMatcher(false).matchAndJoin(space,
                    parsedRecordAndInterpretedFields.getValue(), fieldSchemas,
                    parsedRecordAndInterpretedFields.getKey(), modelSummary, request.isPerformEnrichment(),
                    enrichInternalAttributes, performFetchOnlyForMatching, requestId, isDebug, matchLogs,
                    matchErrorLogs, isCalledViaApiConsole, enforceFuzzyMatch, skipDnBCache);
            Map<String, Object> matchedRecord = extractMap(matchedRecordEnrichmentMap, Matcher.RESULT);
            addMissingFields(fieldSchemas, matchedRecord);
            readyToTransformRecord = matchedRecord;

            if (request.isPerformEnrichment()) {
                enrichmentAttributes = extractMap(matchedRecordEnrichmentMap, Matcher.ENRICHMENT);
            }
        }

        if (enrichmentAttributes == null) {
            enrichmentAttributes = new HashMap<>();
        }

        split("matchRecord");

        Map<String, Object> transformedRecord = null;
        if (shouldSkipScoring) {
            transformedRecord = new HashMap<>();
        } else {
            transformedRecord = transform(scoringArtifacts, readyToTransformRecord);
        }

        split("transformRecord");

        ScoreResponse scoreResponse = null;

        if (shouldSkipScoring) {
            if (isDebug) {
                DebugScoreResponse nonScoreResponse = new DebugScoreResponse();
                nonScoreResponse.setMatchLogs(matchLogs);
                nonScoreResponse.setMatchErrorMessages(matchErrorLogs);
                scoreResponse = nonScoreResponse;
            } else {
                scoreResponse = new ScoreResponse();
            }
        } else {
            if (isDebug) {
                scoreResponse = modelJsonTypeHandler.generateDebugScoreResponse(scoringArtifacts, transformedRecord,
                        readyToTransformRecord, matchLogs, matchErrorLogs);
            } else {
                scoreResponse = modelJsonTypeHandler.generateScoreResponse(scoringArtifacts, transformedRecord);
            }
        }

        scoreResponse.setEnrichmentAttributeValues(enrichmentAttributes);
        scoreResponse.setId(recordId);
        scoreResponse.setTimestamp(timestampFormatter.print(DateTime.now(DateTimeZone.UTC)));

        split("scoreRecord");
        if (shouldPublish) {
            scoreHistoryEntityMgr.publish(space.getTenantId(), request, scoreResponse);
            split("publishScoreHistory");
        }

        return scoreResponse;
    }

    private ModelJsonTypeHandler getModelJsonTypeHandler(String modelJsonType) {
        for (ModelJsonTypeHandler handler : modelJsonTypeHandlers) {
            if (handler.accept(modelJsonType)) {
                return handler;
            }
        }
        throw new LedpException(LedpCode.LEDP_31107, new String[] { modelJsonType });
    }

    @Override
    public List<RecordScoreResponse> process(CustomerSpace space, BulkRecordScoreRequest request, //
            boolean isDebug, boolean enrichInternalAttributes, boolean performFetchOnlyForMatching, String requestId) {
        List<RecordScoreResponse> scoreResponse = new ArrayList<>();
        String requestTimestamp = BaseScoring.dateFormat.format(new Date());

        try {
            split("requestPreparation");
            if (log.isInfoEnabled()) {
                log.info("Received bulk score request for " + request.getRecords().size() + " records");
            }
            request.setRequestTimestamp(requestTimestamp);

            Map<String, Entry<LedpException, ScoringArtifacts>> uniqueScoringArtifactsMap = new HashMap<>();
            Map<String, Map<String, FieldSchema>> uniqueFieldSchemasMap = new HashMap<>();

            fetchModelArtifacts(space, request.getRecords(), uniqueScoringArtifactsMap, uniqueFieldSchemasMap);
            split("retrieveModelArtifacts");

            List<RecordModelTuple> originalOrderParsedTupleList = checkForMissingFields(uniqueScoringArtifactsMap,
                    uniqueFieldSchemasMap, request, performFetchOnlyForMatching);

            split("parseRecord");

            List<RecordModelTuple> partiallyOrderedParsedRecordWithMatchReqList = new ArrayList<>();
            List<RecordModelTuple> partiallyOrderedParsedRecordWithoutMatchReqList = new ArrayList<>();
            List<RecordModelTuple> partiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList = new ArrayList<>();
            List<RecordModelTuple> partiallyOrderedBadRecordList = new ArrayList<>();

            extractParsedList(originalOrderParsedTupleList, uniqueScoringArtifactsMap,
                    partiallyOrderedParsedRecordWithMatchReqList, partiallyOrderedParsedRecordWithoutMatchReqList,
                    partiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList, partiallyOrderedBadRecordList,
                    performFetchOnlyForMatching);
            List<ModelSummary> originalOrderModelSummaryList = extractModelSummaries(originalOrderParsedTupleList,
                    uniqueScoringArtifactsMap);

            Map<RecordModelTuple, Map<String, Object>> unorderedCombinedRecordMap = new HashMap<>();
            Map<RecordModelTuple, Map<String, Object>> unorderedLeadEnrichmentMap = new HashMap<>();
            Map<RecordModelTuple, List<String>> unorderedMatchLogMap = new HashMap<>();
            Map<RecordModelTuple, List<String>> unorderedMatchErrorLogMap = new HashMap<>();

            handleBulkMatchingCases(space, request, isDebug, enrichInternalAttributes, performFetchOnlyForMatching,
                    requestId, uniqueFieldSchemasMap, originalOrderParsedTupleList,
                    partiallyOrderedParsedRecordWithMatchReqList, partiallyOrderedParsedRecordWithoutMatchReqList,
                    partiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList, partiallyOrderedBadRecordList,
                    originalOrderModelSummaryList, unorderedCombinedRecordMap, unorderedLeadEnrichmentMap,
                    unorderedMatchLogMap, unorderedMatchErrorLogMap);

            split("matchRecord");

            Map<RecordModelTuple, Map<String, Object>> unorderedTransformedRecords = transform(
                    uniqueScoringArtifactsMap, unorderedCombinedRecordMap, originalOrderParsedTupleList);
            split("transformRecord");

            if (isDebug) {
                scoreResponse = generateDebugScoreResponse(uniqueScoringArtifactsMap, unorderedTransformedRecords,
                        originalOrderParsedTupleList, unorderedLeadEnrichmentMap, unorderedMatchLogMap,
                        unorderedMatchErrorLogMap);
            } else {
                scoreResponse = generateScoreResponse(uniqueScoringArtifactsMap, unorderedTransformedRecords,
                        originalOrderParsedTupleList, unorderedLeadEnrichmentMap, false, null, null);
            }

            if (log.isInfoEnabled()) {
                log.info("Processed bulk score request for " + request.getRecords().size() + " records");
            }
            split("scoreRecord");
            if (shouldPublish) {
                scoreHistoryEntityMgr.publish(space.getTenantId(), request.getRecords(), scoreResponse);
                split("publishScoreHistory");
            }
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            generateScoreResponseForUnhandledException(scoreResponse, request, ex, requestTimestamp);
        }
        return scoreResponse;

    }

    private void handleBulkMatchingCases(CustomerSpace space, BulkRecordScoreRequest request, boolean isDebug,
            boolean enrichInternalAttributes, boolean performFetchOnlyForMatching, String requestId,
            Map<String, Map<String, FieldSchema>> uniqueFieldSchemasMap,
            List<RecordModelTuple> originalOrderParsedTupleList,
            List<RecordModelTuple> partiallyOrderedParsedRecordWithMatchReqList,
            List<RecordModelTuple> partiallyOrderedParsedRecordWithoutMatchReqList,
            List<RecordModelTuple> partiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList,
            List<RecordModelTuple> partiallyOrderedBadRecordList, List<ModelSummary> originalOrderModelSummaryList,
            Map<RecordModelTuple, Map<String, Object>> unorderedCombinedRecordMap,
            Map<RecordModelTuple, Map<String, Object>> unorderedLeadEnrichmentMap,
            Map<RecordModelTuple, List<String>> unorderedMatchLogMap,
            Map<RecordModelTuple, List<String>> unorderedMatchErrorLogMap) {
        handleBulkMatchAndEnrich(space, request, isDebug, enrichInternalAttributes, performFetchOnlyForMatching,
                requestId, uniqueFieldSchemasMap, originalOrderParsedTupleList,
                partiallyOrderedParsedRecordWithMatchReqList, originalOrderModelSummaryList, unorderedCombinedRecordMap,
                unorderedLeadEnrichmentMap, unorderedMatchLogMap, unorderedMatchErrorLogMap);

        handleBulkEnrichOnly(space, request, isDebug, enrichInternalAttributes, performFetchOnlyForMatching, requestId,
                uniqueFieldSchemasMap, partiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList,
                originalOrderModelSummaryList, unorderedLeadEnrichmentMap, unorderedMatchLogMap,
                unorderedMatchErrorLogMap);

        handleBulkNoMatchAndEnrich(uniqueFieldSchemasMap, originalOrderParsedTupleList,
                partiallyOrderedParsedRecordWithoutMatchReqList, unorderedCombinedRecordMap);

        handleBulkBadRecords(partiallyOrderedBadRecordList, unorderedCombinedRecordMap);
    }

    private void handleBulkBadRecords(List<RecordModelTuple> partiallyOrderedBadRecordList,
            Map<RecordModelTuple, Map<String, Object>> unorderedCombinedRecordMap) {
        if (!partiallyOrderedBadRecordList.isEmpty()) {
            Map<RecordModelTuple, Map<String, Object>> partiallyOrderedBadRecordMap = generateMapForBadRecords(
                    partiallyOrderedBadRecordList);
            unorderedCombinedRecordMap.putAll(partiallyOrderedBadRecordMap);
        }
    }

    private void handleBulkNoMatchAndEnrich(Map<String, Map<String, FieldSchema>> uniqueFieldSchemasMap,
            List<RecordModelTuple> originalOrderParsedTupleList,
            List<RecordModelTuple> partiallyOrderedParsedRecordWithoutMatchReqList,
            Map<RecordModelTuple, Map<String, Object>> unorderedCombinedRecordMap) {
        if (!partiallyOrderedParsedRecordWithoutMatchReqList.isEmpty()) {
            Map<RecordModelTuple, Map<String, Object>> unorderedFormattedRecordWithoutMatchReqMap = format(
                    partiallyOrderedParsedRecordWithoutMatchReqList);
            addMissingFields(uniqueFieldSchemasMap, unorderedFormattedRecordWithoutMatchReqMap,
                    originalOrderParsedTupleList);
            unorderedCombinedRecordMap.putAll(unorderedFormattedRecordWithoutMatchReqMap);
        }
    }

    private void handleBulkEnrichOnly(CustomerSpace space, BulkRecordScoreRequest request, boolean isDebug,
            boolean enrichInternalAttributes, boolean performFetchOnlyForMatching, String requestId,
            Map<String, Map<String, FieldSchema>> uniqueFieldSchemasMap,
            List<RecordModelTuple> partiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList,
            List<ModelSummary> originalOrderModelSummaryList,
            Map<RecordModelTuple, Map<String, Object>> unorderedLeadEnrichmentMap,
            Map<RecordModelTuple, List<String>> unorderedMatchLogMap,
            Map<RecordModelTuple, List<String>> unorderedMatchErrorLogMap) {
        if (!partiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList.isEmpty()) {
            boolean shouldEnrichOnly = true;
            Map<RecordModelTuple, Map<String, Map<String, Object>>> unorderedMatchedRecordEnrichmentMap = getMatcher(
                    true).matchAndJoin(space, partiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList,
                            uniqueFieldSchemasMap, originalOrderModelSummaryList, request.isHomogeneous(),
                            enrichInternalAttributes, performFetchOnlyForMatching, shouldEnrichOnly, isDebug, requestId,
                            unorderedMatchLogMap, unorderedMatchErrorLogMap);

            unorderedLeadEnrichmentMap.putAll(bulkExtractMap(unorderedMatchedRecordEnrichmentMap, Matcher.ENRICHMENT));
        }
    }

    private void handleBulkMatchAndEnrich(CustomerSpace space, BulkRecordScoreRequest request, boolean isDebug,
            boolean enrichInternalAttributes, boolean performFetchOnlyForMatching, String requestId,
            Map<String, Map<String, FieldSchema>> uniqueFieldSchemasMap,
            List<RecordModelTuple> originalOrderParsedTupleList,
            List<RecordModelTuple> partiallyOrderedParsedRecordWithMatchReqList,
            List<ModelSummary> originalOrderModelSummaryList,
            Map<RecordModelTuple, Map<String, Object>> unorderedCombinedRecordMap,
            Map<RecordModelTuple, Map<String, Object>> unorderedLeadEnrichmentMap,
            Map<RecordModelTuple, List<String>> unorderedMatchLogMap,
            Map<RecordModelTuple, List<String>> unorderedMatchErrorLogMap) {
        if (!partiallyOrderedParsedRecordWithMatchReqList.isEmpty()) {
            boolean shouldEnrichOnly = false;
            Map<RecordModelTuple, Map<String, Map<String, Object>>> unorderedMatchedRecordEnrichmentMap = getMatcher(
                    true).matchAndJoin(space, partiallyOrderedParsedRecordWithMatchReqList, uniqueFieldSchemasMap,
                            originalOrderModelSummaryList, request.isHomogeneous(), enrichInternalAttributes,
                            performFetchOnlyForMatching, shouldEnrichOnly, isDebug, requestId, unorderedMatchLogMap,
                            unorderedMatchErrorLogMap);

            Map<RecordModelTuple, Map<String, Object>> unorderedMatchedRecordMap = bulkExtractMap(
                    unorderedMatchedRecordEnrichmentMap, Matcher.RESULT);

            addMissingFields(uniqueFieldSchemasMap, unorderedMatchedRecordMap, originalOrderParsedTupleList);
            unorderedCombinedRecordMap.putAll(unorderedMatchedRecordMap);

            unorderedLeadEnrichmentMap.putAll(bulkExtractMap(unorderedMatchedRecordEnrichmentMap, Matcher.ENRICHMENT));
        }
    }

    private void generateScoreResponseForUnhandledException(List<RecordScoreResponse> scoreResponse,
            BulkRecordScoreRequest request, Exception ex, String requestTimestamp) {
        ScoringApiException apiException = null;
        if (ex instanceof LedpException) {
            apiException = handleLedpException((LedpException) ex);
        } else {
            apiException = new ScoringApiException(LedpCode.LEDP_31111, new String[] { ex.getMessage() });
        }

        for (Record record : request.getRecords()) {
            RecordScoreResponse response = new RecordScoreResponse();
            response.setId(record.getRecordId());
            response.setTimestamp(requestTimestamp);
            List<ScoreModelTuple> scores = new ArrayList<>();
            for (String modelId : record.getModelAttributeValuesMap().keySet()) {
                ScoreModelTuple scoreTuple = new ScoreModelTuple();
                scoreTuple.setModelId(modelId);
                scoreTuple.setError(apiException.getCode().name());
                scoreTuple.setErrorDescription(apiException.getMessage());
                scores.add(scoreTuple);
            }
            response.setScores(scores);
            scoreResponse.add(response);
        }
    }

    private Map<RecordModelTuple, Map<String, Object>> generateMapForBadRecords(
            List<RecordModelTuple> partiallyOrderedBadRecordList) {
        Map<RecordModelTuple, Map<String, Object>> map = new HashMap<>();
        for (RecordModelTuple tuple : partiallyOrderedBadRecordList) {
            map.put(tuple, null);
        }
        return map;
    }

    private Map<RecordModelTuple, Map<String, Object>> bulkExtractMap(
            Map<RecordModelTuple, Map<String, Map<String, Object>>> unorderedMatchedRecordEnrichmentMap, String key) {
        Map<RecordModelTuple, Map<String, Object>> map = new HashMap<>();
        for (RecordModelTuple tupleKey : unorderedMatchedRecordEnrichmentMap.keySet()) {
            Map<String, Object> dataMap = unorderedMatchedRecordEnrichmentMap.get(tupleKey).get(key);
            if (dataMap != null) {
                map.put(tupleKey, dataMap);
            }
        }
        return map;
    }

    private Map<RecordModelTuple, Map<String, Object>> format(
            List<RecordModelTuple> partiallyOrderedParsedRecordWithoutMatchReqList) {
        Map<RecordModelTuple, Map<String, Object>> formattedMap = new HashMap<>();
        for (RecordModelTuple tuple : partiallyOrderedParsedRecordWithoutMatchReqList) {
            formattedMap.put(tuple, tuple.getParsedData().getKey());
        }
        return formattedMap;
    }

    protected void extractParsedList(List<RecordModelTuple> parsedTupleList, //
            Map<String, Entry<LedpException, ScoringArtifacts>> scoringArtifactsMap, //
            List<RecordModelTuple> partiallyOrderedParsedRecordWithMatchReqList, //
            List<RecordModelTuple> partiallyOrderedParsedRecordWithoutMatchReqList, //
            List<RecordModelTuple> partiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList, //
            List<RecordModelTuple> partiallyOrderedBadRecordList, //
            boolean performFetchOnlyForMatching) {
        for (RecordModelTuple tuple : parsedTupleList) {
            extractParsedTuple(scoringArtifactsMap, //
                    partiallyOrderedParsedRecordWithMatchReqList, //
                    partiallyOrderedParsedRecordWithoutMatchReqList, //
                    partiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList, //
                    partiallyOrderedBadRecordList, tuple, performFetchOnlyForMatching);
        }
    }

    private void extractParsedTuple(
            Map<String, //
                    Entry<LedpException, ScoringArtifacts>> scoringArtifactsMap, //
            List<RecordModelTuple> partiallyOrderedParsedRecordWithMatchReqList, //
            List<RecordModelTuple> partiallyOrderedParsedRecordWithoutMatchReqList, //
            List<RecordModelTuple> partiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList, //
            List<RecordModelTuple> partiallyOrderedBadRecordList, RecordModelTuple tuple, //
            boolean performFetchOnlyForMatching) {
        // put this tuple in either partiallyOrderedPmmlParsedRecordList or in
        // partiallyOrderedParsedRecordWithMatchReqList based on model json type

        String modelId = tuple.getModelId();
        ScoringArtifacts scoringArtifacts = scoringArtifactsMap.get(modelId).getValue();
        boolean shouldSkipMatching = shouldSkipMatching(scoringArtifacts, performFetchOnlyForMatching);

        if (tuple.getException() != null || //
                scoringArtifactsMap.get(tuple.getModelId()).getValue() == null) {
            partiallyOrderedBadRecordList.add(tuple);
        } else if (shouldSkipMatching) {
            if (tuple.getRecord().isPerformEnrichment()) {
                partiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList.add(tuple);
            } else {
                partiallyOrderedParsedRecordWithoutMatchReqList.add(tuple);
            }
        } else {
            partiallyOrderedParsedRecordWithMatchReqList.add(tuple);
        }
    }

    protected List<ModelSummary> extractModelSummaries(List<RecordModelTuple> originalOrderParsedTupleList,
            Map<String, Entry<LedpException, ScoringArtifacts>> scoringArtifactsMap) {
        List<ModelSummary> originalOrderModelSummaryList = new ArrayList<>();
        for (RecordModelTuple tuple : originalOrderParsedTupleList) {
            String modelId = tuple.getModelId();
            if (org.apache.commons.lang.StringUtils.isNotEmpty(modelId)) {
                ModelSummary modelSummary = null;
                if (scoringArtifactsMap.get(modelId).getValue() != null) {
                    modelSummary = scoringArtifactsMap.get(modelId).getValue().getModelSummary();
                }
                originalOrderModelSummaryList.add(modelSummary);
            } else {
                originalOrderModelSummaryList.add(null);
            }
        }
        return originalOrderModelSummaryList;
    }

    protected List<RecordScoreResponse> generateDebugScoreResponse(
            Map<String, Entry<LedpException, ScoringArtifacts>> uniqueScoringArtifactsMap,
            Map<RecordModelTuple, Map<String, Object>> unorderedTransformedRecords,
            List<RecordModelTuple> originalOrderParsedTupleList,
            Map<RecordModelTuple, Map<String, Object>> unorderedLeadEnrichmentMap,
            Map<RecordModelTuple, List<String>> unorderedMatchLogMap,
            Map<RecordModelTuple, List<String>> unorderedMatchErrorLogMap) {
        int idx = 0;

        List<RecordScoreResponse> debugResponseList = new ArrayList<>();
        List<RecordScoreResponse> result = generateScoreResponse(uniqueScoringArtifactsMap, unorderedTransformedRecords,
                originalOrderParsedTupleList, unorderedLeadEnrichmentMap, true, unorderedMatchLogMap,
                unorderedMatchErrorLogMap);
        try {
            for (RecordScoreResponse recordResponse : result) {
                DebugRecordScoreResponse debugRecordResponse = new DebugRecordScoreResponse(recordResponse);
                Map<String, Map<String, Object>> transformedDataMap = new HashMap<>();

                for (ScoreModelTuple scoreModelTuple : recordResponse.getScores()) {
                    RecordModelTuple tuple = originalOrderParsedTupleList.get(idx++);
                    Map<String, Object> value = unorderedTransformedRecords.get(tuple);
                    transformedDataMap.put(scoreModelTuple.getModelId(), value);
                    debugRecordResponse.setMatchLogs(unorderedMatchLogMap.get(tuple));
                    debugRecordResponse.setMatchErrorMessages(unorderedMatchErrorLogMap.get(tuple));
                }

                debugRecordResponse.setTransformedRecordMap(transformedDataMap);
                debugResponseList.add(debugRecordResponse);
            }
            return debugResponseList;
        } catch (Exception ex) {
            // in case of any error just default to regular result
            log.error(ex.getMessage(), ex);
            return result;
        }
    }

    protected List<RecordScoreResponse> generateScoreResponse(
            Map<String, Entry<LedpException, ScoringArtifacts>> uniqueScoringArtifactsMap,
            Map<RecordModelTuple, Map<String, Object>> unorderedTransformedRecords,
            List<RecordModelTuple> originalOrderParsedTupleList,
            Map<RecordModelTuple, Map<String, Object>> unorderedLeadEnrichmentMap, boolean isDebugMode,
            Map<RecordModelTuple, List<String>> unorderedMatchLogMap,
            Map<RecordModelTuple, List<String>> unorderedMatchErrorLogMap) {
        Map<String, RecordScoreResponse> responseMap = new HashMap<>();
        List<RecordScoreResponse> responseList = new ArrayList<>();
        for (RecordModelTuple tuple : originalOrderParsedTupleList) {
            String latticeId = tuple.getLatticeId();
            String recordId = tuple.getRecord().getRecordId();
            Map<String, Object> transformedRecord = null;
            ScoreResponse resp = null;
            List<ScoreModelTuple> scores = null;
            RecordScoreResponse recordResp = null;

            try {
                if (responseMap.get(recordId) == null) {
                    recordResp = new RecordScoreResponse();
                    responseMap.put(recordId, recordResp);
                    responseList.add(recordResp);
                    recordResp.setLatticeId(latticeId);

                    recordResp.setId(recordId);

                    recordResp.setTimestamp(tuple.getRequstTimestamp());

                    scores = new ArrayList<>();
                    recordResp.setScores(scores);
                    recordResp.setWarnings(getWarnings(recordId));
                } else {
                    recordResp = responseMap.get(recordId);
                    scores = recordResp.getScores();
                }

                if (tuple.getException() == null) {
                    transformedRecord = unorderedTransformedRecords.get(tuple);

                    ScoringArtifacts scoringArtifacts = getScoringArtifactForTuple(uniqueScoringArtifactsMap, tuple);
                    ModelJsonTypeHandler ModelJsonTypeHandler = getModelJsonTypeHandler(
                            scoringArtifacts.getModelJsonType());
                    if (isDebugMode) {
                        resp = ModelJsonTypeHandler.generateDebugScoreResponse(scoringArtifacts, transformedRecord,
                                null, unorderedMatchLogMap.get(tuple), unorderedMatchErrorLogMap.get(tuple));
                    } else {
                        resp = ModelJsonTypeHandler.generateScoreResponse(scoringArtifacts, transformedRecord);
                    }
                } else {
                    if (log.isInfoEnabled()) {
                        log.info("Skip scoring for " + tuple.getRecord().getRecordId() + "_" + tuple.getModelId()
                                + " as it already has error: " + tuple.getException().getMessage());
                    }
                }

                if (recordResp != null && tuple.getException() == null
                        && recordResp.getEnrichmentAttributeValues() == null
                        && tuple.getRecord().isPerformEnrichment()) {
                    Map<String, Object> enrichmentList = unorderedLeadEnrichmentMap.get(tuple);
                    if (enrichmentList == null) {
                        enrichmentList = new HashMap<>();
                    }
                    recordResp.setEnrichmentAttributeValues(enrichmentList);
                }
            } catch (LedpException ex) {
                tuple.setException(handleLedpException(ex));
            } catch (Exception ex) {
                log.error(ex.getMessage(), ex);
                tuple.setException(new ScoringApiException(LedpCode.LEDP_31108, new String[] { ex.getMessage() }));
            }

            ScoreModelTuple score = new ScoreModelTuple();
            score.setModelId(tuple.getModelId());

            ScoringApiException exception = tuple.getException();

            if (exception == null) {
                score.setScore(resp.getScore());
                score.setBucket(resp.getBucket());
                if (isDebugMode) {
                    score.setProbability(((DebugScoreResponse) resp).getProbability());
                }
            } else {
                score.setError(exception.getCode().getExternalCode());
                score.setErrorDescription(exception.getMessage());
            }
            scores.add(score);
        }
        return responseList;
    }

    private ScoringArtifacts getScoringArtifactForTuple(
            Map<String, Entry<LedpException, ScoringArtifacts>> uniqueScoringArtifactsMap, RecordModelTuple tuple) {
        String modelId = tuple.getModelId();
        ScoringArtifacts scoringArtifacts = null;

        if (uniqueScoringArtifactsMap.get(modelId) != null) {
            scoringArtifacts = uniqueScoringArtifactsMap.get(modelId).getValue();
        }

        return scoringArtifacts;
    }

    private Map<String, FieldSchema> getSchemaForTuple(Map<String, Map<String, FieldSchema>> fieldSchemasMap,
            RecordModelTuple tuple) {
        String modelId = tuple.getModelId();
        Map<String, FieldSchema> schema = fieldSchemasMap.get(modelId);
        return schema;
    }

    protected Map<RecordModelTuple, Map<String, Object>> transform(
            Map<String, Entry<LedpException, ScoringArtifacts>> uniqueScoringArtifactsMap,
            Map<RecordModelTuple, Map<String, Object>> unorderedCombinedRecordMap,
            List<RecordModelTuple> originalOrderParsedTupleList) {
        Map<RecordModelTuple, Map<String, Object>> resultMap = new HashMap<>();
        for (RecordModelTuple tuple : unorderedCombinedRecordMap.keySet()) {
            try {
                Map<String, Object> matchedRecord = unorderedCombinedRecordMap.get(tuple);

                ScoringArtifacts scoringArtifacts = getScoringArtifactForTuple(uniqueScoringArtifactsMap, tuple);
                resultMap.put(tuple, transform(scoringArtifacts, matchedRecord));
            } catch (LedpException ex) {
                tuple.setException(handleLedpException(ex));
                resultMap.put(tuple, null);
            } catch (Exception ex) {
                log.error(ex.getMessage(), ex);
                tuple.setException(new ScoringApiException(LedpCode.LEDP_31110, new String[] { ex.getMessage() }));
                resultMap.put(tuple, null);
            }
        }
        return resultMap;
    }

    protected void addMissingFields(Map<String, Map<String, FieldSchema>> fieldSchemasMap, //
            Map<RecordModelTuple, Map<String, Object>> matchedRecords, //
            List<RecordModelTuple> parsedTupleList) {
        for (RecordModelTuple tuple : matchedRecords.keySet()) {
            Map<String, Object> matchedRecord = matchedRecords.get(tuple);
            Map<String, FieldSchema> schema = getSchemaForTuple(fieldSchemasMap, tuple);
            addMissingFields(schema, matchedRecord);
        }
    }

    protected List<RecordModelTuple> checkForMissingFields(
            Map<String, Entry<LedpException, ScoringArtifacts>> uniqueScoringArtifactsMap,
            Map<String, Map<String, FieldSchema>> fieldSchemasMap, BulkRecordScoreRequest request,
            boolean performFetchOnlyForMatching) {
        List<RecordModelTuple> recordAndFieldList = new ArrayList<>();
        for (Record record : request.getRecords()) {
            for (String modelId : record.getModelAttributeValuesMap().keySet()) {
                Map<String, FieldSchema> fieldSchemas = fieldSchemasMap.get(modelId);
                Map<String, Object> attrValMap = record.getModelAttributeValuesMap().get(modelId);
                Entry<LedpException, ScoringArtifacts> modelArtifactTuple = uniqueScoringArtifactsMap.get(modelId);
                String latticeId = record.getRecordId();
                SimpleEntry<Map<String, Object>, InterpretedFields> parsedRecord = null;
                ScoringApiException missingEssentialFieldsOrBadModelException = null;

                if (modelArtifactTuple.getKey() == null) {
                    ScoringArtifacts scoringArtifact = modelArtifactTuple.getValue();

                    ModelJsonTypeHandler modelJsonTypeHandler = getModelJsonTypeHandler(
                            scoringArtifact.getModelJsonType());

                    if (!shouldSkipMatching(scoringArtifact, performFetchOnlyForMatching)) {
                        missingEssentialFieldsOrBadModelException = checkForMissingFields(record.getRecordId(),
                                scoringArtifact, fieldSchemas, attrValMap, modelJsonTypeHandler, modelId);
                    }

                    if (!Record.LATTICE_ID.equals(record.getIdType())) {
                        latticeId = LatticeIdGenerator.generateLatticeId(attrValMap);
                    }

                    record.setRequestTimestamp(request.getRequestTimestamp());
                    try {
                        parsedRecord = modelJsonTypeHandler.parseRecord(record.getRecordId(), fieldSchemas, attrValMap,
                                modelId);
                    } catch (LedpException ex) {
                        missingEssentialFieldsOrBadModelException = handleLedpException(ex);
                    } catch (Exception ex) {
                        log.error(ex.getMessage(), ex);
                        missingEssentialFieldsOrBadModelException = new ScoringApiException(LedpCode.LEDP_31109,
                                new String[] { ex.getMessage() });
                    }
                } else {
                    LedpException ex = modelArtifactTuple.getKey();
                    missingEssentialFieldsOrBadModelException = handleLedpException(ex);
                }

                RecordModelTuple tuple = new RecordModelTuple(request.getRequestTimestamp(), latticeId, record,
                        parsedRecord, modelId, missingEssentialFieldsOrBadModelException);
                recordAndFieldList.add(tuple);

            }
        }

        return recordAndFieldList;
    }

    protected ScoringApiException handleLedpException(LedpException ex) {
        log.error(ex.getMessage(), ex);
        ScoringApiException missingEssentialFieldsOrBadModelException = null;
        if (ex instanceof ScoringApiException) {
            missingEssentialFieldsOrBadModelException = (ScoringApiException) ex;
        } else {
            missingEssentialFieldsOrBadModelException = new ScoringApiException(ex.getCode(),
                    new String[] { ex.getMessage() });
        }
        return missingEssentialFieldsOrBadModelException;
    }

    protected void fetchModelArtifacts(CustomerSpace space, List<Record> records,
            Map<String, java.util.Map.Entry<LedpException, ScoringArtifacts>> uniqueScoringArtifactsMap,
            Map<String, Map<String, FieldSchema>> uniqueFieldSchemasMap) {
        // extract a set of unique modelIds across all modelIds
        Set<String> uniqueModelIds = new HashSet<>();
        for (Record record : records) {
            for (String modelId : record.getModelAttributeValuesMap().keySet()) {
                // check if modelId is valid
                if (org.apache.commons.lang.StringUtils.isBlank(modelId)) {
                    throw new ScoringApiException(LedpCode.LEDP_31101);
                }
                uniqueModelIds.add(modelId);
            }
        }

        // for unique modelIds, extract artifacts (via cache and in parallel)
        // for now it is simple for loop
        for (String modelId : uniqueModelIds) {
            ScoringArtifacts scoringArtifacts = null;
            LedpException ex = null;
            Map<String, FieldSchema> fieldSchema = null;

            try {
                scoringArtifacts = modelRetriever.getModelArtifacts(space, modelId);
                fieldSchema = scoringArtifacts.getFieldSchemas();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                if (e instanceof LedpException) {
                    ex = (LedpException) e;
                } else {
                    Throwable t = e.getCause();
                    if (t != null && t instanceof LedpException) {
                        ex = (LedpException) t;
                    } else {
                        ex = new LedpException(LedpCode.LEDP_31102,
                                new String[] { "Error while fetching " + modelId + ": " + e.getMessage() });
                    }
                }
            }

            java.util.Map.Entry<LedpException, ScoringArtifacts> entry = new java.util.AbstractMap.SimpleEntry<>(ex,
                    scoringArtifacts);
            uniqueScoringArtifactsMap.put(modelId, entry);
            uniqueFieldSchemasMap.put(modelId, fieldSchema);
        }
    }

    private String getIdIfAvailable(InterpretedFields interpretedFields, Map<String, Object> record) {
        String value = "";
        if (!org.apache.commons.lang.StringUtils.isBlank(interpretedFields.getRecordId())) {
            Object id = record.get(interpretedFields.getRecordId());
            if (!StringStandardizationUtils.objectIsNullOrEmptyString(id)) {
                value = String.valueOf(id);
            }
        }
        return value;
    }

    private AbstractMap.SimpleEntry<Map<String, Object>, InterpretedFields> parseRecord(
            Map<String, FieldSchema> fieldSchemas, Map<String, Object> record,
            ModelJsonTypeHandler modelJsonTypeHandler) {
        return parseRecord(null, fieldSchemas, record, modelJsonTypeHandler);
    }

    private AbstractMap.SimpleEntry<Map<String, Object>, InterpretedFields> parseRecord(String recordId,
            Map<String, FieldSchema> fieldSchemas, Map<String, Object> record,
            ModelJsonTypeHandler modelJsonTypeHandler) {
        return modelJsonTypeHandler.parseRecord(null, fieldSchemas, record, null);
    }

    private void addMissingFields(Map<String, FieldSchema> fieldSchemas, Map<String, Object> record) {
        // Ensure all non-tranform keys are represented in the record
        for (String fieldName : fieldSchemas.keySet()) {
            if (fieldSchemas.get(fieldName).source != FieldSource.TRANSFORMS) {
                if (!record.containsKey(fieldName)) {
                    record.put(fieldName, null);
                }
            }
        }
        if (log.isDebugEnabled()) {
            log.debug(JsonUtils.serialize(record));
        }
    }

    private ScoringApiException checkForMissingFields(ScoringArtifacts scoringArtifact,
            Map<String, FieldSchema> fieldSchemas, Map<String, Object> record,
            ModelJsonTypeHandler modelJsonTypeHandler) {
        return checkForMissingFields(null, scoringArtifact, fieldSchemas, record, modelJsonTypeHandler, null);
    }

    private ScoringApiException checkForMissingFields(String recordId, ScoringArtifacts scoringArtifact,
            Map<String, FieldSchema> fieldSchemas, Map<String, Object> record,
            ModelJsonTypeHandler modelJsonTypeHandler, String modelId) {
        List<String> missingMatchFields = new ArrayList<>();
        List<String> missingFields = new ArrayList<>();

        EnumSet<FieldInterpretation> expectedDomainFields = EnumSet.of(FieldInterpretation.Domain,
                FieldInterpretation.Email, FieldInterpretation.Website);
        EnumSet<FieldInterpretation> expectedMatchFields = EnumSet.of(FieldInterpretation.Country,
                FieldInterpretation.CompanyName, FieldInterpretation.State, FieldInterpretation.DUNS);
        expectedMatchFields.addAll(expectedDomainFields);
        boolean hasOneOfDomain = false;
        // PropData does not use City and will default Country to US
        boolean hasKeyFieldForMatching = false;

        Map<String, FieldSchema> combinedFieldSchema = new HashMap<>();
        combinedFieldSchema.putAll(modelJsonTypeHandler.getDefaultFieldSchemaForMatch());
        combinedFieldSchema.putAll(fieldSchemas);

        for (String fieldName : combinedFieldSchema.keySet()) {
            FieldSchema fieldSchema = combinedFieldSchema.get(fieldName);

            if (fieldSchema.source != FieldSource.REQUEST) {
                continue;
            }
            if (!record.containsKey(fieldName) && fieldSchemas.containsKey(fieldName)) {
                missingFields.add(fieldName);
            }

            Object fieldValue = record.get(fieldName);
            if (expectedMatchFields.contains(fieldSchema.interpretation)
                    && StringStandardizationUtils.objectIsNullOrEmptyString(fieldValue)) {
                missingMatchFields.add(fieldName);
            }
            if (expectedDomainFields.contains(fieldSchema.interpretation)
                    && !StringStandardizationUtils.objectIsNullOrEmptyString(fieldValue)) {
                hasOneOfDomain = true;
            }

            if ((fieldSchema.interpretation == FieldInterpretation.CompanyName
                    || fieldSchema.interpretation == FieldInterpretation.DUNS)
                    && !StringStandardizationUtils.objectIsNullOrEmptyString(fieldValue)) {
                hasKeyFieldForMatching = true;
            }
        }

        ScoringApiException missingEssentialFieldsException = modelJsonTypeHandler.checkForMissingEssentialFields(
                recordId, modelId, //
                hasOneOfDomain, hasKeyFieldForMatching, missingMatchFields);

        if (!missingFields.isEmpty()) {
            addWarning(WarningCode.MISSING_COLUMN, recordId, missingFields, modelId);
        }

        return missingEssentialFieldsException;
    }

    private Map<String, Object> transform(ScoringArtifacts scoringArtifacts, Map<String, Object> matchedRecord) {
        if (matchedRecord == null) {
            return null;
        }

        Map<String, Object> standardTransformedRecord = recordTransformer.transform(
                scoringArtifacts.getModelArtifactsDir().getAbsolutePath(),
                scoringArtifacts.getEventTableDataComposition().transforms, matchedRecord);

        Map<String, Object> datascienceTransformedRecord = recordTransformer.transform(
                scoringArtifacts.getModelArtifactsDir().getAbsolutePath(),
                scoringArtifacts.getDataScienceDataComposition().transforms, standardTransformedRecord);
        return datascienceTransformedRecord;
    }

    protected List<Warning> getWarnings(String recordId) {
        return warnings.getWarnings(recordId);
    }

    protected void addWarning(WarningCode code, String recordId, List<String> fields, String modelId) {
        warnings.addWarning(recordId,
                new Warning(code, new String[] { getWarningPrefix(modelId) + Joiner.on(",").join(fields) }));
    }

    protected String getWarningPrefix(String modelId) {
        return StringStandardizationUtils.objectIsNullOrEmptyString(modelId) ? ""
                : "[For ModelId - " + modelId + "] => ";
    }

    private boolean shouldSkipMatching(ScoringArtifacts scoringArtifacts, boolean performFetchOnlyForMatching) {
        boolean shouldSkipMatching = false;
        if (performFetchOnlyForMatching) {
            return shouldSkipMatching;
        }
        if (scoringArtifacts != null) {
            ModelSummaryProvenance provenance = scoringArtifacts.getModelSummary().getModelSummaryConfiguration();
            for (ModelSummaryProvenanceProperty property : scoringArtifacts.getModelSummary()
                    .getModelSummaryProvenanceProperties()) {
                if (property.getOption().equals(ProvenancePropertyName.ExcludePropdataColumns.name())) {
                    shouldSkipMatching = provenance.getBoolean(ProvenancePropertyName.ExcludePropdataColumns);
                    break;
                }
            }
            boolean isPmmlModel = ModelJsonTypeHandler.PMML_MODEL.equals(scoringArtifacts.getModelJsonType());

            // add code here

            shouldSkipMatching = shouldSkipMatching || isPmmlModel;
        }
        return shouldSkipMatching;
    }
}
