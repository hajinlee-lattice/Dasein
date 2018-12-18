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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
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
import com.latticeengines.scoringapi.score.AdditionalScoreConfig;
import com.latticeengines.scoringapi.score.BulkMatchingContext;
import com.latticeengines.scoringapi.score.ScoreRequestProcessor;
import com.latticeengines.scoringapi.score.SingleMatchingContext;
import com.latticeengines.scoringapi.transform.RecordTransformer;

@Component("scoreRequestProcessor")
public class ScoreRequestProcessorImpl extends BaseRequestProcessorImpl implements ScoreRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(ScoreRequestProcessorImpl.class);

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
    public ScoreResponse process(ScoreRequest request, AdditionalScoreConfig additionalScoreConfig) {
        split("requestPreparation");
        requestInfo.put("Rule", Strings.nullToEmpty(request.getRule()));
        requestInfo.put("Source", Strings.nullToEmpty(request.getSource()));

        SingleMatchingContext singleMatchingConfig = SingleMatchingContext.instance();

        Pair<Boolean, Boolean> skipFlags = performModelHandling(request, additionalScoreConfig, singleMatchingConfig);

        boolean shouldSkipMatching = skipFlags.getLeft();
        boolean shouldSkipScoring = skipFlags.getRight();

        AbstractMap.SimpleEntry<Map<String, Object>, InterpretedFields> parsedRecordAndInterpretedFields = parseRecord(
                singleMatchingConfig.getFieldSchemas(), request.getRecord(),
                singleMatchingConfig.getModelJsonTypeHandler(), additionalScoreConfig.getRequestId());

        String recordId = getIdIfAvailable(parsedRecordAndInterpretedFields.getValue(), request.getRecord());
        requestInfo.put("RecordId", recordId);
        split("parseRecord");

        performMatch(request, additionalScoreConfig, shouldSkipMatching, singleMatchingConfig, shouldSkipScoring,
                parsedRecordAndInterpretedFields);

        split("matchRecord");

        performTransform(singleMatchingConfig, shouldSkipScoring);

        split("transformRecord");

        ScoreResponse scoreResponse = performScoring(additionalScoreConfig, singleMatchingConfig, shouldSkipScoring,
                recordId);

        split("scoreRecord");

        performaPublish(request, additionalScoreConfig, scoreResponse);

        return scoreResponse;
    }

    @Override
    public List<RecordScoreResponse> process(BulkRecordScoreRequest request,
            AdditionalScoreConfig additionalScoreConfig) {
        List<RecordScoreResponse> scoreResponse = new ArrayList<>();
        String requestTimestamp = DateTimeUtils.convertToStringUTCISO8601(new Date());

        try {
            split("requestPreparation");
            if (log.isInfoEnabled()) {
                log.info("Received bulk score request for " + request.getRecords().size() + " records");
            }
            request.setRequestTimestamp(requestTimestamp);

            BulkMatchingContext bulkMatchingConfig = BulkMatchingContext.instance();

            fetchModelArtifacts(additionalScoreConfig, request.getRecords(), bulkMatchingConfig);
            split("retrieveModelArtifacts");

            bulkMatchingConfig.setOriginalOrderParsedTupleList(
                    checkForMissingFields(bulkMatchingConfig, request, additionalScoreConfig));

            split("parseRecord");

            extractParsedList(additionalScoreConfig, bulkMatchingConfig);

            bulkMatchingConfig.setOriginalOrderModelSummaryList(extractModelSummaries(bulkMatchingConfig));

            handleBulkMatchingCases(request, additionalScoreConfig, bulkMatchingConfig);

            split("matchRecord");

            bulkMatchingConfig.setUnorderedTransformedRecords(transform(bulkMatchingConfig));
            split("transformRecord");

            if (additionalScoreConfig.isDebug()) {
                scoreResponse = generateDebugScoreResponse(additionalScoreConfig, bulkMatchingConfig);
            } else {
                scoreResponse = generateScoreResponse(additionalScoreConfig, bulkMatchingConfig);
            }

            if (log.isInfoEnabled()) {
                log.info("Processed bulk score request for " + request.getRecords().size() + " records");
            }
            split("scoreRecord");
            if (shouldPublish) {
                scoreHistoryEntityMgr.publish(additionalScoreConfig.getSpace().getTenantId(), request.getRecords(),
                        scoreResponse);
                split("publishScoreHistory");
            }
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            generateScoreResponseForUnhandledException(scoreResponse, request, ex, requestTimestamp);
        }
        return scoreResponse;

    }

    private void handleBulkMatchingCases(BulkRecordScoreRequest request, AdditionalScoreConfig additionalScoreConfig,
            BulkMatchingContext bulkMatchingConfig) {
        handleBulkMatchAndEnrich(request, additionalScoreConfig, bulkMatchingConfig);

        handleBulkEnrichOnly(request, additionalScoreConfig, bulkMatchingConfig);

        handleBulkNoMatchAndEnrich(bulkMatchingConfig);

        handleBulkBadRecords(bulkMatchingConfig);
    }

    private void handleBulkBadRecords(BulkMatchingContext bulkMatchingConfig) {
        if (!bulkMatchingConfig.getPartiallyOrderedBadRecordList().isEmpty()) {
            Map<RecordModelTuple, Map<String, Object>> partiallyOrderedBadRecordMap = generateMapForBadRecords(
                    bulkMatchingConfig.getPartiallyOrderedBadRecordList());
            bulkMatchingConfig.getUnorderedCombinedRecordMap().putAll(partiallyOrderedBadRecordMap);
        }
    }

    private void handleBulkNoMatchAndEnrich(BulkMatchingContext bulkMatchingConfig) {
        if (!bulkMatchingConfig.getPartiallyOrderedParsedRecordWithoutMatchReqList().isEmpty()) {
            Map<RecordModelTuple, Map<String, Object>> unorderedFormattedRecordWithoutMatchReqMap = format(
                    bulkMatchingConfig.getPartiallyOrderedParsedRecordWithoutMatchReqList());
            addMissingFields(bulkMatchingConfig, unorderedFormattedRecordWithoutMatchReqMap);
            bulkMatchingConfig.getUnorderedCombinedRecordMap().putAll(unorderedFormattedRecordWithoutMatchReqMap);
        }
    }

    private void handleBulkEnrichOnly(BulkRecordScoreRequest request, AdditionalScoreConfig additionalScoreConfig,
            BulkMatchingContext bulkMatchingConfig) {
        if (!bulkMatchingConfig.getPartiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList().isEmpty()) {
            boolean shouldEnrichOnly = true;
            Map<RecordModelTuple, Map<String, Map<String, Object>>> unorderedMatchedRecordEnrichmentMap = getMatcher(
                    true).matchAndJoin(additionalScoreConfig, bulkMatchingConfig,
                            bulkMatchingConfig.getPartiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList(),
                            shouldEnrichOnly);

            Map<RecordModelTuple, Map<String, Object>> partiallyOrderedParsedRecordWithEnrichButWithoutMatchReqMap = format(
                    bulkMatchingConfig.getPartiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList());
            addMissingFields(bulkMatchingConfig, partiallyOrderedParsedRecordWithEnrichButWithoutMatchReqMap);
            bulkMatchingConfig.getUnorderedCombinedRecordMap()
                    .putAll(partiallyOrderedParsedRecordWithEnrichButWithoutMatchReqMap);

            bulkMatchingConfig.getUnorderedLeadEnrichmentMap()
                    .putAll(bulkExtractMap(unorderedMatchedRecordEnrichmentMap, Matcher.ENRICHMENT));
        }
    }

    private void handleBulkMatchAndEnrich(BulkRecordScoreRequest request, AdditionalScoreConfig additionalScoreConfig,
            BulkMatchingContext bulkMatchingConfig) {
        if (!bulkMatchingConfig.getPartiallyOrderedParsedRecordWithMatchReqList().isEmpty()) {
            boolean shouldEnrichOnly = false;
            Map<RecordModelTuple, Map<String, Map<String, Object>>> unorderedMatchedRecordEnrichmentMap = getMatcher(
                    true).matchAndJoin(additionalScoreConfig, bulkMatchingConfig,
                            bulkMatchingConfig.getPartiallyOrderedParsedRecordWithMatchReqList(), shouldEnrichOnly);

            Map<RecordModelTuple, Map<String, Object>> unorderedMatchedRecordMap = bulkExtractMap(
                    unorderedMatchedRecordEnrichmentMap, Matcher.RESULT);

            addMissingFields(bulkMatchingConfig, unorderedMatchedRecordMap);
            bulkMatchingConfig.getUnorderedCombinedRecordMap().putAll(unorderedMatchedRecordMap);

            bulkMatchingConfig.getUnorderedLeadEnrichmentMap()
                    .putAll(bulkExtractMap(unorderedMatchedRecordEnrichmentMap, Matcher.ENRICHMENT));
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

    protected void extractParsedList(AdditionalScoreConfig additionalScoreConfig,
            BulkMatchingContext bulkMatchingConfig) {
        Matcher matcher = getMatcher(true);
        List<LeadEnrichmentAttribute> enrichmentAttributeMetadataList = matcher.getEnrichmentMetadata(
                additionalScoreConfig.getSpace(), bulkMatchingConfig.getOriginalOrderParsedTupleList(),
                additionalScoreConfig.isEnrichInternalAttributes());

        for (RecordModelTuple tuple : bulkMatchingConfig.getOriginalOrderParsedTupleList()) {
            extractParsedTuple(additionalScoreConfig, bulkMatchingConfig, tuple,
                    CollectionUtils.isEmpty(enrichmentAttributeMetadataList));
        }
    }

    private void extractParsedTuple(AdditionalScoreConfig additionalScoreConfig, BulkMatchingContext bulkMatchingConfig,
            RecordModelTuple tuple, boolean isEnrichmentMetadataListEmpty) {
        // put this tuple in either partiallyOrderedPmmlParsedRecordList or in
        // partiallyOrderedParsedRecordWithMatchReqList based on model json type

        String modelId = tuple.getModelId();
        ScoringArtifacts scoringArtifacts = bulkMatchingConfig.getUniqueScoringArtifactsMap().get(modelId).getValue();
        boolean shouldSkipMatching = shouldSkipMatching(scoringArtifacts,
                additionalScoreConfig.isPerformFetchOnlyForMatching(), additionalScoreConfig.isForceSkipMatching());

        if (tuple.getException() != null || //
                bulkMatchingConfig.getUniqueScoringArtifactsMap().get(tuple.getModelId()).getValue() == null) {
            bulkMatchingConfig.getPartiallyOrderedBadRecordList().add(tuple);
        } else if (shouldSkipMatching) {
            if (!isEnrichmentMetadataListEmpty && tuple.getRecord().isPerformEnrichment()) {
                bulkMatchingConfig.getPartiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList().add(tuple);
            } else {
                bulkMatchingConfig.getPartiallyOrderedParsedRecordWithoutMatchReqList().add(tuple);
            }
        } else {
            bulkMatchingConfig.getPartiallyOrderedParsedRecordWithMatchReqList().add(tuple);
        }
    }

    protected List<ModelSummary> extractModelSummaries(BulkMatchingContext bulkMatchingConfig) {
        List<ModelSummary> originalOrderModelSummaryList = new ArrayList<>();
        for (RecordModelTuple tuple : bulkMatchingConfig.getOriginalOrderParsedTupleList()) {
            String modelId = tuple.getModelId();
            if (StringUtils.isNotEmpty(modelId)) {
                ModelSummary modelSummary = null;
                if (bulkMatchingConfig.getUniqueScoringArtifactsMap().get(modelId).getValue() != null) {
                    modelSummary = bulkMatchingConfig.getUniqueScoringArtifactsMap().get(modelId).getValue()
                            .getModelSummary();
                }
                originalOrderModelSummaryList.add(modelSummary);
            } else {
                originalOrderModelSummaryList.add(null);
            }
        }
        return originalOrderModelSummaryList;
    }

    protected List<RecordScoreResponse> generateDebugScoreResponse(AdditionalScoreConfig additionalScoreConfig,
            BulkMatchingContext bulkMatchingConfig) {
        int idx = 0;

        List<RecordScoreResponse> debugResponseList = new ArrayList<>();
        List<RecordScoreResponse> result = generateScoreResponse(additionalScoreConfig, bulkMatchingConfig);
        try {
            for (RecordScoreResponse recordResponse : result) {
                DebugRecordScoreResponse debugRecordResponse = new DebugRecordScoreResponse(recordResponse);
                Map<String, Map<String, Object>> transformedDataMap = new HashMap<>();

                for (ScoreModelTuple scoreModelTuple : recordResponse.getScores()) {
                    RecordModelTuple tuple = bulkMatchingConfig.getOriginalOrderParsedTupleList().get(idx++);
                    Map<String, Object> value = bulkMatchingConfig.getUnorderedTransformedRecords().get(tuple);
                    transformedDataMap.put(scoreModelTuple.getModelId(), value);
                    debugRecordResponse.setMatchLogs(bulkMatchingConfig.getUnorderedMatchLogMap().get(tuple));
                    debugRecordResponse
                            .setMatchErrorMessages(bulkMatchingConfig.getUnorderedMatchErrorLogMap().get(tuple));
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

    protected List<RecordScoreResponse> generateScoreResponse(AdditionalScoreConfig additionalScoreConfig,
            BulkMatchingContext bulkMatchingConfig) {
        Map<String, RecordScoreResponse> responseMap = new HashMap<>();
        List<RecordScoreResponse> responseList = new ArrayList<>();
        for (RecordModelTuple tuple : bulkMatchingConfig.getOriginalOrderParsedTupleList()) {
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
                    transformedRecord = bulkMatchingConfig.getUnorderedTransformedRecords().get(tuple);

                    ScoringArtifacts scoringArtifacts = getScoringArtifactForTuple(
                            bulkMatchingConfig.getUniqueScoringArtifactsMap(), tuple);
                    ModelJsonTypeHandler ModelJsonTypeHandler = getModelJsonTypeHandler(
                            scoringArtifacts.getModelJsonType());
                    if (additionalScoreConfig.isDebug()) {
                        resp = ModelJsonTypeHandler.generateDebugScoreResponse(scoringArtifacts, transformedRecord,
                                null, bulkMatchingConfig.getUnorderedMatchLogMap().get(tuple),
                                bulkMatchingConfig.getUnorderedMatchErrorLogMap().get(tuple));
                    } else {
                        resp = ModelJsonTypeHandler.generateScoreResponse(scoringArtifacts, transformedRecord,
                                additionalScoreConfig.isCalledViaInternalResource());
                    }
                } else {
                    if (log.isInfoEnabled()) {
                        log.info(String.format("Skip scoring for recordId=%s, modelId=%s, as it already has error=%s",
                                tuple.getRecord().getRecordId(), tuple.getModelId(),
                                tuple.getException().getMessage()));
                    }
                }

                if (recordResp != null && tuple.getException() == null
                        && recordResp.getEnrichmentAttributeValues() == null
                        && tuple.getRecord().isPerformEnrichment()) {
                    Map<String, Object> enrichmentList = bulkMatchingConfig.getUnorderedLeadEnrichmentMap().get(tuple);
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
                if (additionalScoreConfig.isDebug() || additionalScoreConfig.isCalledViaInternalResource()) {
                    score.setProbability(((DebugScoreResponse) resp).getProbability());
                }
            } else {
                score.setError(exception.getCode().toString());
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

    protected Map<RecordModelTuple, Map<String, Object>> transform(BulkMatchingContext bulkMatchingConfig) {
        Map<RecordModelTuple, Map<String, Object>> resultMap = new HashMap<>();
        for (RecordModelTuple tuple : bulkMatchingConfig.getUnorderedCombinedRecordMap().keySet()) {
            try {
                Map<String, Object> matchedRecord = bulkMatchingConfig.getUnorderedCombinedRecordMap().get(tuple);

                ScoringArtifacts scoringArtifacts = getScoringArtifactForTuple(
                        bulkMatchingConfig.getUniqueScoringArtifactsMap(), tuple);
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

    protected void addMissingFields(BulkMatchingContext bulkMatchingConfig,
            Map<RecordModelTuple, Map<String, Object>> matchedRecords) {
        for (RecordModelTuple tuple : bulkMatchingConfig.getMatchedRecords().keySet()) {
            Map<String, Object> matchedRecord = bulkMatchingConfig.getMatchedRecords().get(tuple);
            Map<String, FieldSchema> schema = getSchemaForTuple(bulkMatchingConfig.getUniqueFieldSchemasMap(), tuple);
            addMissingFields(schema, matchedRecord);
        }
    }

    protected List<RecordModelTuple> checkForMissingFields(BulkMatchingContext bulkMatchingConfig,
            BulkRecordScoreRequest request, AdditionalScoreConfig additionalScoreConfig) {
        List<RecordModelTuple> recordAndFieldList = new ArrayList<>();
        for (Record record : request.getRecords()) {
            for (String modelId : record.getModelAttributeValuesMap().keySet()) {
                Map<String, FieldSchema> fieldSchemas = bulkMatchingConfig.getUniqueFieldSchemasMap().get(modelId);
                Map<String, Object> attrValMap = record.getModelAttributeValuesMap().get(modelId);
                Entry<LedpException, ScoringArtifacts> modelArtifactTuple = bulkMatchingConfig
                        .getUniqueScoringArtifactsMap().get(modelId);
                String latticeId = record.getRecordId();
                SimpleEntry<Map<String, Object>, InterpretedFields> parsedRecord = null;
                ScoringApiException missingEssentialFieldsOrBadModelException = null;

                if (modelArtifactTuple.getKey() == null) {
                    ScoringArtifacts scoringArtifact = modelArtifactTuple.getValue();

                    ModelJsonTypeHandler modelJsonTypeHandler = getModelJsonTypeHandler(
                            scoringArtifact.getModelJsonType());

                    if (!shouldSkipMatching(scoringArtifact, additionalScoreConfig.isPerformFetchOnlyForMatching(),
                            additionalScoreConfig.isForceSkipMatching())) {
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
            missingEssentialFieldsOrBadModelException = new ScoringApiException(ex.getCode(), ex.getMessage());
        }
        return missingEssentialFieldsOrBadModelException;
    }

    protected void fetchModelArtifacts(AdditionalScoreConfig additionalScoreConfig, List<Record> records,
            BulkMatchingContext bulkMatchingConfig) {
        // extract a set of unique modelIds across all modelIds
        Set<String> uniqueModelIds = new HashSet<>();
        for (Record record : records) {
            for (String modelId : record.getModelAttributeValuesMap().keySet()) {
                // check if modelId is valid
                if (StringUtils.isBlank(modelId)) {
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
                scoringArtifacts = modelRetriever.getModelArtifacts(additionalScoreConfig.getSpace(), modelId);
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
            bulkMatchingConfig.getUniqueScoringArtifactsMap().put(modelId, entry);
            bulkMatchingConfig.getUniqueFieldSchemasMap().put(modelId, fieldSchema);
        }
    }

    private String getIdIfAvailable(InterpretedFields interpretedFields, Map<String, Object> record) {
        String value = "";
        if (!StringUtils.isBlank(interpretedFields.getRecordId())) {
            Object id = record.get(interpretedFields.getRecordId());
            if (!StringStandardizationUtils.objectIsNullOrEmptyString(id)) {
                value = String.valueOf(id);
            }
        }
        return value;
    }

    private AbstractMap.SimpleEntry<Map<String, Object>, InterpretedFields> parseRecord(
            Map<String, FieldSchema> fieldSchemas, Map<String, Object> record,
            ModelJsonTypeHandler modelJsonTypeHandler, String requestId) {
        return parseRecord(requestId, fieldSchemas, record, modelJsonTypeHandler);
    }

    private AbstractMap.SimpleEntry<Map<String, Object>, InterpretedFields> parseRecord(String recordId,
            Map<String, FieldSchema> fieldSchemas, Map<String, Object> record,
            ModelJsonTypeHandler modelJsonTypeHandler) {
        return modelJsonTypeHandler.parseRecord(recordId, fieldSchemas, record, null);
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
            ModelJsonTypeHandler modelJsonTypeHandler, String requestId) {
        return checkForMissingFields(requestId, scoringArtifact, fieldSchemas, record, modelJsonTypeHandler, null);
    }

    private ScoringApiException checkForMissingFields(String recordId, ScoringArtifacts scoringArtifact,
            Map<String, FieldSchema> fieldSchemas, Map<String, Object> record,
            ModelJsonTypeHandler modelJsonTypeHandler, String modelId) {
        List<String> missingMatchFields = new ArrayList<>();
        List<String> missingFields = new ArrayList<>();

        EnumSet<FieldInterpretation> expectedDomainFields = EnumSet.of(FieldInterpretation.Domain,
                FieldInterpretation.Email, FieldInterpretation.Website);
        EnumSet<FieldInterpretation> expectedMatchFields = EnumSet.of(FieldInterpretation.CompanyName,
                FieldInterpretation.DUNS);
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

    private boolean shouldSkipMatching(ScoringArtifacts scoringArtifacts, boolean performFetchOnlyForMatching,
            boolean forceSkipMatching) {
        // priority to this flag
        if (forceSkipMatching) {
            return true;
        }
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

    private Pair<Boolean, Boolean> performModelHandling(ScoreRequest request,
            AdditionalScoreConfig additionalScoreConfig, SingleMatchingContext singleMatchingConfig) {
        Boolean shouldSkipMatching = false;
        Boolean shouldSkipScoring = false;
        if (!additionalScoreConfig.isCalledViaApiConsole()
                || !StringStandardizationUtils.objectIsNullOrEmptyString(request.getModelId())) {
            shouldSkipMatching = performModelArtifactRetrieval(request, additionalScoreConfig, singleMatchingConfig);

            split("retrieveModelArtifacts");

            singleMatchingConfig.setModelJsonTypeHandler( //
                    getModelJsonTypeHandler(singleMatchingConfig.getScoringArtifacts().getModelJsonType()));

            if (!shouldSkipMatching) {
                ScoringApiException missingEssentialFieldsException = checkForMissingFields(
                        singleMatchingConfig.getScoringArtifacts(), singleMatchingConfig.getFieldSchemas(),
                        request.getRecord(), singleMatchingConfig.getModelJsonTypeHandler(),
                        additionalScoreConfig.getRequestId());
                if (missingEssentialFieldsException != null) {
                    if (!additionalScoreConfig.isPerformFetchOnlyForMatching()
                            || StringStandardizationUtils.objectIsNullOrEmptyString(
                                    request.getRecord().get(FieldInterpretation.LatticeAccountId.toString()))) {
                        throw missingEssentialFieldsException;
                    }
                }
            }
        } else {
            shouldSkipScoring = true;
            singleMatchingConfig.setFieldSchemas(new HashMap<>());
            singleMatchingConfig
                    .setModelJsonTypeHandler(getModelJsonTypeHandler("NOT" + ModelJsonTypeHandler.PMML_MODEL));
        }
        return new ImmutablePair<Boolean, Boolean>(shouldSkipMatching, shouldSkipScoring);
    }

    private boolean performModelArtifactRetrieval(ScoreRequest request, AdditionalScoreConfig additionalScoreConfig,
            SingleMatchingContext singleMatchingConfig) {
        boolean shouldSkipMatching = false;
        if (StringUtils.isBlank(request.getModelId())) {
            throw new ScoringApiException(LedpCode.LEDP_31101);
        }

        singleMatchingConfig.setScoringArtifacts( //
                modelRetriever.getModelArtifacts(additionalScoreConfig.getSpace(), request.getModelId()));

        requestInfo.put("ModelId", singleMatchingConfig.getScoringArtifacts().getModelSummary().getId());
        requestInfo.put("ModelName", singleMatchingConfig.getScoringArtifacts().getModelSummary().getDisplayName());

        if (singleMatchingConfig.getScoringArtifacts().getModelSummary().getStatus() != ModelSummaryStatus.ACTIVE) {
            throw new ScoringApiException(LedpCode.LEDP_31114,
                    new String[] { singleMatchingConfig.getScoringArtifacts().getModelSummary().getId() });
        }
        requestInfo.put("ModelType", (singleMatchingConfig.getScoringArtifacts().getModelType() == null ? ""
                : singleMatchingConfig.getScoringArtifacts().getModelType().name()));
        shouldSkipMatching = shouldSkipMatching(singleMatchingConfig.getScoringArtifacts(),
                additionalScoreConfig.isPerformFetchOnlyForMatching(), additionalScoreConfig.isForceSkipMatching());

        singleMatchingConfig.setFieldSchemas(singleMatchingConfig.getScoringArtifacts().getFieldSchemas());
        return shouldSkipMatching;
    }

    private void performaPublish(ScoreRequest request, AdditionalScoreConfig additionalScoreConfig,
            ScoreResponse scoreResponse) {
        if (shouldPublish) {
            scoreHistoryEntityMgr.publish(additionalScoreConfig.getSpace().getTenantId(), request, scoreResponse);
            split("publishScoreHistory");
        }
    }

    private void performTransform(SingleMatchingContext singleMatchingConfig, boolean shouldSkipScoring) {
        if (!shouldSkipScoring) {
            singleMatchingConfig.setTransformedRecord(transform(singleMatchingConfig.getScoringArtifacts(),
                    singleMatchingConfig.getReadyToTransformRecord()));
        }
    }

    private void performMatch(ScoreRequest request, AdditionalScoreConfig additionalScoreConfig,
            boolean shouldSkipMatching, SingleMatchingContext singleMatchingConfig, boolean shouldSkipScoring,
            AbstractMap.SimpleEntry<Map<String, Object>, InterpretedFields> parsedRecordAndInterpretedFields) {
        if (shouldSkipMatching) {
            if (request.isPerformEnrichment()) {
                // pass null model summary for performing matching for
                // enrichment only
                Map<String, Map<String, Object>> matchedRecordEnrichmentMap = getMatcher(false).matchAndJoin(
                        additionalScoreConfig, singleMatchingConfig, parsedRecordAndInterpretedFields.getValue(),
                        parsedRecordAndInterpretedFields.getKey(), true);
                Map<String, Object> matchedRecord = extractMap(matchedRecordEnrichmentMap, Matcher.RESULT);
                addMissingFields(singleMatchingConfig.getFieldSchemas(), matchedRecord);

                singleMatchingConfig //
                        .setReadyToTransformRecord(matchedRecord)
                        .setEnrichmentAttributes(extractMap(matchedRecordEnrichmentMap, Matcher.ENRICHMENT));

            } else {
                Map<String, Object> formattedRecord = parsedRecordAndInterpretedFields.getKey();
                addMissingFields(singleMatchingConfig.getFieldSchemas(), formattedRecord);
                singleMatchingConfig //
                        .setReadyToTransformRecord(formattedRecord);
            }
        } else {
            if (!shouldSkipScoring) {
                singleMatchingConfig.setModelSummary(singleMatchingConfig.getScoringArtifacts().getModelSummary());
            }

            Map<String, Map<String, Object>> matchedRecordEnrichmentMap = getMatcher(false).matchAndJoin(
                    additionalScoreConfig, singleMatchingConfig, parsedRecordAndInterpretedFields.getValue(),
                    parsedRecordAndInterpretedFields.getKey(), request.isPerformEnrichment());
            Map<String, Object> matchedRecord = extractMap(matchedRecordEnrichmentMap, Matcher.RESULT);
            addMissingFields(singleMatchingConfig.getFieldSchemas(), matchedRecord);

            singleMatchingConfig //
                    .setReadyToTransformRecord(matchedRecord);

            if (request.isPerformEnrichment()) {
                singleMatchingConfig //
                        .setEnrichmentAttributes(extractMap(matchedRecordEnrichmentMap, Matcher.ENRICHMENT));
            }
        }
    }

    private ScoreResponse performScoring(AdditionalScoreConfig additionalScoreConfig,
            SingleMatchingContext singleMatchingConfig, boolean shouldSkipScoring, String recordId) {
        ScoreResponse scoreResponse = null;

        if (shouldSkipScoring) {
            if (additionalScoreConfig.isDebug()) {
                DebugScoreResponse nonScoreResponse = new DebugScoreResponse();
                nonScoreResponse.setMatchLogs(singleMatchingConfig.getMatchLogs());
                nonScoreResponse.setMatchErrorMessages(singleMatchingConfig.getMatchErrorLogs());
                scoreResponse = nonScoreResponse;
            } else {
                scoreResponse = new ScoreResponse();
            }
        } else {
            if (additionalScoreConfig.isDebug()) {
                scoreResponse = singleMatchingConfig.getModelJsonTypeHandler().generateDebugScoreResponse(
                        singleMatchingConfig.getScoringArtifacts(), singleMatchingConfig.getTransformedRecord(),
                        singleMatchingConfig.getReadyToTransformRecord(), singleMatchingConfig.getMatchLogs(),
                        singleMatchingConfig.getMatchErrorLogs());
            } else {
                scoreResponse = singleMatchingConfig.getModelJsonTypeHandler().generateScoreResponse(
                        singleMatchingConfig.getScoringArtifacts(), singleMatchingConfig.getTransformedRecord(),
                        additionalScoreConfig.isCalledViaInternalResource());
            }
        }

        scoreResponse.setEnrichmentAttributeValues(singleMatchingConfig.getEnrichmentAttributes());
        scoreResponse.setId(recordId);
        scoreResponse.setTimestamp(timestampFormatter.print(DateTime.now(DateTimeZone.UTC)));

        if (warnings.hasWarnings(additionalScoreConfig.getRequestId())) {
            scoreResponse.setWarnings(warnings.getWarnings(additionalScoreConfig.getRequestId()));
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
}
