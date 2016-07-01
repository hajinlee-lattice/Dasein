package com.latticeengines.scoringapi.score.impl;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.latticeengines.common.exposed.rest.HttpStopWatch;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.StringUtils;
import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.domain.exposed.scoringapi.Record;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse.ScoreModelTuple;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.Warning;
import com.latticeengines.domain.exposed.scoringapi.WarningCode;
import com.latticeengines.domain.exposed.scoringapi.Warnings;
import com.latticeengines.scoringapi.entitymanager.ScoreHistoryEntityMgr;
import com.latticeengines.scoringapi.exposed.DebugRecordScoreResponse;
import com.latticeengines.scoringapi.exposed.InterpretedFields;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;
import com.latticeengines.scoringapi.exposed.context.RequestInfo;
import com.latticeengines.scoringapi.exposed.exception.ScoringApiException;
import com.latticeengines.scoringapi.exposed.model.ModelJsonTypeHandler;
import com.latticeengines.scoringapi.exposed.model.ModelRetriever;
import com.latticeengines.scoringapi.match.Matcher;
import com.latticeengines.scoringapi.score.ScoreRequestProcessor;
import com.latticeengines.scoringapi.transform.RecordTransformer;
import com.latticeengines.scoringinternalapi.controller.BaseScoring;

@Component("scoreRequestProcessor")
public class ScoreRequestProcessorImpl implements ScoreRequestProcessor {
    private static final Log log = LogFactory.getLog(ScoreRequestProcessorImpl.class);

    @Autowired
    private HttpStopWatch httpStopWatch;

    @Autowired
    private Matcher matcher;

    @Autowired
    private ModelRetriever modelRetriever;

    @Autowired
    private RecordTransformer recordTransformer;

    @Autowired
    private Warnings warnings;

    @Autowired
    private RequestInfo requestInfo;

    @Autowired
    private List<ModelJsonTypeHandler> modelJsonTypeHandlers;

    @Autowired
    private ScoreHistoryEntityMgr scoreHistoryEntityMgr;

    private DateTimeFormatter timestampFormatter = ISODateTimeFormat.dateTime();

    @Override
    public ScoreResponse process(CustomerSpace space, ScoreRequest request, boolean isDebug) {
        split("requestPreparation");
        if (org.apache.commons.lang.StringUtils.isBlank(request.getModelId())) {
            throw new ScoringApiException(LedpCode.LEDP_31101);
        }

        requestInfo.put("Rule", Strings.nullToEmpty(request.getRule()));
        requestInfo.put("Source", Strings.nullToEmpty(request.getSource()));

        ScoringArtifacts scoringArtifacts = modelRetriever.getModelArtifacts(space, request.getModelId());
        requestInfo.put("ModelId", scoringArtifacts.getModelSummary().getId());
        requestInfo.put("ModelName", scoringArtifacts.getModelSummary().getName());
        requestInfo.put("ModelType",
                (scoringArtifacts.getModelType() == null ? "" : scoringArtifacts.getModelType().name()));

        Map<String, FieldSchema> fieldSchemas = scoringArtifacts.getFieldSchemas();
        split("retrieveModelArtifacts");

        ModelJsonTypeHandler modelJsonTypeHandler = getModelJsonTypeHandler(scoringArtifacts.getModelJsonType());

        ScoringApiException missingEssentialFieldsException = checkForMissingFields(scoringArtifacts, fieldSchemas,
                request.getRecord(), modelJsonTypeHandler);
        if (missingEssentialFieldsException != null) {
            throw missingEssentialFieldsException;
        }

        AbstractMap.SimpleEntry<Map<String, Object>, InterpretedFields> parsedRecordAndInterpretedFields = parseRecord(
                fieldSchemas, request.getRecord());
        String recordId = getIdIfAvailable(parsedRecordAndInterpretedFields.getValue(), request.getRecord());
        requestInfo.put("RecordId", recordId);
        split("parseRecord");

        Map<String, Object> readyToTransformRecord = null;

        if (!ModelJsonTypeHandler.PMML_MODEL.equals(scoringArtifacts.getModelJsonType())) {
            Map<String, Object> matchedRecord = matcher.matchAndJoin(space, parsedRecordAndInterpretedFields.getValue(),
                    fieldSchemas, parsedRecordAndInterpretedFields.getKey(), scoringArtifacts.getModelSummary());
            addMissingFields(fieldSchemas, matchedRecord);
            readyToTransformRecord = matchedRecord;
        } else {
            Map<String, Object> formattedPmmlRecord = parsedRecordAndInterpretedFields.getKey();
            addMissingFields(fieldSchemas, formattedPmmlRecord);
            readyToTransformRecord = formattedPmmlRecord;
        }
        split("matchRecord");

        Map<String, Object> transformedRecord = transform(scoringArtifacts, readyToTransformRecord);
        split("transformRecord");

        ScoreResponse scoreResponse = null;
        if (isDebug) {
            scoreResponse = modelJsonTypeHandler.generateDebugScoreResponse(scoringArtifacts, transformedRecord,
                    readyToTransformRecord);
        } else {
            scoreResponse = modelJsonTypeHandler.generateScoreResponse(scoringArtifacts, transformedRecord);
        }
        scoreResponse.setId(recordId);
        scoreResponse.setTimestamp(timestampFormatter.print(DateTime.now(DateTimeZone.UTC)));
        scoreHistoryEntityMgr.publish(request, scoreResponse);
        split("scoreRecord");

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
    public List<RecordScoreResponse> process(CustomerSpace space, BulkRecordScoreRequest request, boolean isDebug) {
        split("requestPreparation");
        if (log.isInfoEnabled()) {
            log.info("Received bulk score request for " + request.getRecords().size() + " records");
        }
        String requestTimestamp = BaseScoring.dateFormat.format(new Date());

        request.setRequestTimestamp(requestTimestamp);

        Map<String, ScoringArtifacts> uniqueScoringArtifactsMap = new HashMap<>();
        Map<String, Map<String, FieldSchema>> uniqueFieldSchemasMap = new HashMap<>();

        fetchModelArtifacts(space, request.getRecords(), uniqueScoringArtifactsMap, uniqueFieldSchemasMap);
        split("retrieveModelArtifacts");

        List<RecordModelTuple> originalOrderParsedTupleList = checkForMissingFields(uniqueScoringArtifactsMap,
                uniqueFieldSchemasMap, request);

        split("parseRecord");

        List<RecordModelTuple> partiallyOrderedParsedTupleList = new ArrayList<>();
        List<RecordModelTuple> partiallyOrderedPmmlParsedRecordList = new ArrayList<>();

        extractParsedList(originalOrderParsedTupleList, uniqueScoringArtifactsMap, partiallyOrderedParsedTupleList,
                partiallyOrderedPmmlParsedRecordList);
        List<ModelSummary> originalOrderModelSummaryList = extractModelSummaries(originalOrderParsedTupleList,
                uniqueScoringArtifactsMap);

        Map<RecordModelTuple, Map<String, Object>> unorderedCombinedRecordMap = new HashMap<>();

        if (!partiallyOrderedParsedTupleList.isEmpty()) {
            Map<RecordModelTuple, Map<String, Object>> unorderedMatchedRecordMap = bulkMatchAndJoin(space,
                    uniqueFieldSchemasMap, partiallyOrderedParsedTupleList, originalOrderModelSummaryList);
            addMissingFields(uniqueFieldSchemasMap, unorderedMatchedRecordMap, originalOrderParsedTupleList);
            unorderedCombinedRecordMap.putAll(unorderedMatchedRecordMap);
        }
        if (!partiallyOrderedPmmlParsedRecordList.isEmpty()) {
            Map<RecordModelTuple, Map<String, Object>> unorderedFormattedPmmlRecordMap = format(
                    partiallyOrderedPmmlParsedRecordList);
            addMissingFields(uniqueFieldSchemasMap, unorderedFormattedPmmlRecordMap, originalOrderParsedTupleList);
            unorderedCombinedRecordMap.putAll(unorderedFormattedPmmlRecordMap);
        }

        split("matchRecord");

        Map<RecordModelTuple, Map<String, Object>> unorderedTransformedRecords = transform(uniqueScoringArtifactsMap,
                unorderedCombinedRecordMap, originalOrderParsedTupleList);
        split("transformRecord");

        List<RecordScoreResponse> scoreResponse = new ArrayList<>();
        if (isDebug) {
            scoreResponse = generateDebugScoreResponse(uniqueScoringArtifactsMap, unorderedTransformedRecords,
                    originalOrderParsedTupleList);
        } else {
            scoreResponse = generateScoreResponse(uniqueScoringArtifactsMap, unorderedTransformedRecords,
                    originalOrderParsedTupleList);
        }

        scoreHistoryEntityMgr.publish(request.getRecords(),  scoreResponse);

        split("scoreRecord");
        if (log.isInfoEnabled()) {
            log.info("Processed bulk score request for " + request.getRecords().size() + " records");
        }
        return scoreResponse;
    }

    Map<RecordModelTuple, Map<String, Object>> bulkMatchAndJoin(CustomerSpace space,
            Map<String, Map<String, FieldSchema>> uniqueFieldSchemasMap,
            List<RecordModelTuple> partiallyOrderedParsedTupleList, List<ModelSummary> originalOrderModelSummaryList) {
        Map<RecordModelTuple, Map<String, Object>> unorderedMatchedRecordMap = matcher.matchAndJoin(space,
                partiallyOrderedParsedTupleList, uniqueFieldSchemasMap, originalOrderModelSummaryList);
        return unorderedMatchedRecordMap;
    }

    private Map<RecordModelTuple, Map<String, Object>> format(
            List<RecordModelTuple> partiallyOrderedPmmlParsedRecordList) {
        Map<RecordModelTuple, Map<String, Object>> formattedMap = new HashMap<>();
        for (RecordModelTuple tuple : partiallyOrderedPmmlParsedRecordList) {
            formattedMap.put(tuple, tuple.getParsedData().getKey());
        }
        return formattedMap;
    }

    void extractParsedList(List<RecordModelTuple> parsedTupleList, //
            Map<String, ScoringArtifacts> scoringArtifactsMap, //
            List<RecordModelTuple> partiallyOrderedParsedTupleList, //
            List<RecordModelTuple> partiallyOrderedPmmlParsedRecordList) {
        for (RecordModelTuple tuple : parsedTupleList) {
            if (tuple.getException() == null) {
                extractParsedTuple(scoringArtifactsMap, partiallyOrderedParsedTupleList,
                        partiallyOrderedPmmlParsedRecordList, tuple);
            }
        }
    }

    private void extractParsedTuple(Map<String, ScoringArtifacts> scoringArtifactsMap, //
            List<RecordModelTuple> partiallyOrderedParsedTupleList, //
            List<RecordModelTuple> partiallyOrderedPmmlParsedRecordList, //
            RecordModelTuple tuple) {
        // put this tuple in either partiallyOrderedPmmlParsedRecordList or in
        // partiallyOrderedParsedTupleList based on model json type
        if (ModelJsonTypeHandler.PMML_MODEL.equals(scoringArtifactsMap.get(tuple.getModelId()).getModelJsonType())) {
            partiallyOrderedPmmlParsedRecordList.add(tuple);
        } else {
            partiallyOrderedParsedTupleList.add(tuple);
        }
    }

    List<ModelSummary> extractModelSummaries(List<RecordModelTuple> originalOrderParsedTupleList,
            Map<String, ScoringArtifacts> scoringArtifactsMap) {
        List<ModelSummary> originalOrderModelSummaryList = new ArrayList<>();
        for (RecordModelTuple tuple : originalOrderParsedTupleList) {
            String modelId = tuple.getModelId();
            if (org.apache.commons.lang.StringUtils.isNotEmpty(modelId)) {
                ModelSummary modelSummary = scoringArtifactsMap.get(modelId).getModelSummary();
                originalOrderModelSummaryList.add(modelSummary);
            } else {
                originalOrderModelSummaryList.add(null);
            }
        }
        return originalOrderModelSummaryList;
    }

    List<RecordScoreResponse> generateDebugScoreResponse(Map<String, ScoringArtifacts> uniqueScoringArtifactsMap,
            Map<RecordModelTuple, Map<String, Object>> unorderedTransformedRecords,
            List<RecordModelTuple> originalOrderParsedTupleList) {
        int idx = 0;

        List<RecordScoreResponse> debugResponseList = new ArrayList<>();
        List<RecordScoreResponse> result = generateScoreResponse(uniqueScoringArtifactsMap, unorderedTransformedRecords,
                originalOrderParsedTupleList);
        try {
            for (RecordScoreResponse recordResponse : result) {
                DebugRecordScoreResponse debugRecordResponse = new DebugRecordScoreResponse(recordResponse);
                Map<String, Map<String, Object>> transformedDataMap = new HashMap<>();

                for (ScoreModelTuple scoreModelTuple : recordResponse.getScores()) {
                    RecordModelTuple tuple = originalOrderParsedTupleList.get(idx++);
                    Map<String, Object> value = unorderedTransformedRecords.get(tuple);
                    transformedDataMap.put(scoreModelTuple.getModelId(), value);
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

    List<RecordScoreResponse> generateScoreResponse(Map<String, ScoringArtifacts> uniqueScoringArtifactsMap,
            Map<RecordModelTuple, Map<String, Object>> unorderedTransformedRecords,
            List<RecordModelTuple> originalOrderParsedTupleList) {
        Map<String, RecordScoreResponse> responseMap = new HashMap<>();
        List<RecordScoreResponse> responseList = new ArrayList<>();
        for (RecordModelTuple tuple : originalOrderParsedTupleList) {
            String latticeId = tuple.getLatticeId();
            String recordId = tuple.getRecord().getRecordId();
            Map<String, Object> transformedRecord = null;
            ScoreResponse resp = null;

            if (tuple.getException() == null) {
                transformedRecord = unorderedTransformedRecords.get(tuple);
                if (log.isInfoEnabled()) {
                    log.info("Start scoring for " + tuple.getRecord().getRecordId() + "_" + tuple.getModelId());
                }

                ScoringArtifacts scoringArtifacts = getScoringArtifactForTuple(uniqueScoringArtifactsMap, tuple);
                ModelJsonTypeHandler ModelJsonTypeHandler = getModelJsonTypeHandler(
                        scoringArtifacts.getModelJsonType());

                resp = ModelJsonTypeHandler.generateScoreResponse(scoringArtifacts, transformedRecord);

            } else {
                if (log.isInfoEnabled()) {
                    log.info("Skip scoring for " + tuple.getRecord().getRecordId() + "_" + tuple.getModelId()
                            + " as it already has error: " + tuple.getException().getMessage());
                }
            }

            List<ScoreModelTuple> scores = null;
            RecordScoreResponse recordResp = null;

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
            if (recordResp != null && tuple.getException() == null && recordResp.getEnrichmentAttributeValues() == null
                    && tuple.getRecord().isPerformEnrichment()) {
                recordResp.setEnrichmentAttributeValues(transformedRecord);
            }

            ScoreModelTuple score = new ScoreModelTuple();
            score.setModelId(tuple.getModelId());

            ScoringApiException exception = tuple.getException();

            if (exception == null) {
                score.setScore(resp.getScore());
                if (log.isInfoEnabled()) {
                    log.info("Completed scoring for " + tuple.getRecord().getRecordId() + "_" + tuple.getModelId());
                }
            } else {
                score.setError(exception.getCode().getExternalCode());
                score.setErrorDescription(exception.getMessage());
            }
            scores.add(score);
        }
        return responseList;
    }

    private ScoringArtifacts getScoringArtifactForTuple(Map<String, ScoringArtifacts> uniqueScoringArtifactsMap,
            RecordModelTuple tuple) {
        String modelId = tuple.getModelId();
        ScoringArtifacts scoringArtifacts = uniqueScoringArtifactsMap.get(modelId);
        return scoringArtifacts;
    }

    private Map<String, FieldSchema> getSchemaForTuple(Map<String, Map<String, FieldSchema>> fieldSchemasMap,
            RecordModelTuple tuple) {
        String modelId = tuple.getModelId();
        Map<String, FieldSchema> schema = fieldSchemasMap.get(modelId);
        return schema;
    }

    Map<RecordModelTuple, Map<String, Object>> transform(Map<String, ScoringArtifacts> uniqueScoringArtifactsMap,
            Map<RecordModelTuple, Map<String, Object>> unorderedCombinedRecordMap,
            List<RecordModelTuple> originalOrderParsedTupleList) {
        Map<RecordModelTuple, Map<String, Object>> resultMap = new HashMap<>();
        for (RecordModelTuple tuple : unorderedCombinedRecordMap.keySet()) {
            Map<String, Object> matchedRecord = unorderedCombinedRecordMap.get(tuple);

            ScoringArtifacts scoringArtifacts = getScoringArtifactForTuple(uniqueScoringArtifactsMap, tuple);
            resultMap.put(tuple, transform(scoringArtifacts, matchedRecord));
        }
        return resultMap;
    }

    void addMissingFields(Map<String, Map<String, FieldSchema>> fieldSchemasMap, //
            Map<RecordModelTuple, Map<String, Object>> matchedRecords, //
            List<RecordModelTuple> parsedTupleList) {
        for (RecordModelTuple tuple : matchedRecords.keySet()) {
            Map<String, Object> matchedRecord = matchedRecords.get(tuple);
            Map<String, FieldSchema> schema = getSchemaForTuple(fieldSchemasMap, tuple);
            addMissingFields(schema, matchedRecord);
        }
    }

    List<RecordModelTuple> checkForMissingFields(Map<String, ScoringArtifacts> scoringArtifactsMap,
            Map<String, Map<String, FieldSchema>> fieldSchemasMap, BulkRecordScoreRequest request) {
        List<RecordModelTuple> recordAndFieldList = new ArrayList<>();
        for (Record record : request.getRecords()) {
            for (String modelId : record.getModelAttributeValuesMap().keySet()) {
                Map<String, FieldSchema> fieldSchemas = fieldSchemasMap.get(modelId);
                Map<String, Object> attrValMap = record.getModelAttributeValuesMap().get(modelId);
                ScoringArtifacts scoringArtifact = scoringArtifactsMap.get(modelId);
                ModelJsonTypeHandler modelJsonTypeHandler = getModelJsonTypeHandler(scoringArtifact.getModelJsonType());
                ScoringApiException missingEssentialFieldsException = checkForMissingFields(record.getRecordId(),
                        scoringArtifact, fieldSchemas, attrValMap, modelJsonTypeHandler, modelId);
                String latticeId = record.getRecordId();

                if (!Record.LATTICE_ID.equals(record.getIdType())) {
                    latticeId = LatticeIdGenerator.generateLatticeId(attrValMap);
                }

                record.setRequestTimestamp(request.getRequestTimestamp());

                RecordModelTuple tuple = new RecordModelTuple(request.getRequestTimestamp(), latticeId, record,
                        parseRecord(record.getRecordId(), fieldSchemas, attrValMap, modelId), modelId,
                        missingEssentialFieldsException);
                recordAndFieldList.add(tuple);

            }
        }

        return recordAndFieldList;
    }

    void fetchModelArtifacts(CustomerSpace space, List<Record> records,
            Map<String, ScoringArtifacts> uniqueScoringArtifactsMap,
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
            ScoringArtifacts scoringArtifacts = modelRetriever.getModelArtifacts(space, modelId);
            uniqueScoringArtifactsMap.put(modelId, scoringArtifacts);
            uniqueFieldSchemasMap.put(modelId, scoringArtifacts.getFieldSchemas());
        }
    }

    private String getIdIfAvailable(InterpretedFields interpretedFields, Map<String, Object> record) {
        String value = "";
        if (!org.apache.commons.lang.StringUtils.isBlank(interpretedFields.getRecordId())) {
            Object id = record.get(interpretedFields.getRecordId());
            if (!StringUtils.objectIsNullOrEmptyString(id)) {
                value = String.valueOf(id);
            }
        }
        return value;
    }

    private void split(String key) {
        httpStopWatch.split(key);
        if (log.isInfoEnabled()) {
            log.info(key);
        }
    }

    private AbstractMap.SimpleEntry<Map<String, Object>, InterpretedFields> parseRecord(
            Map<String, FieldSchema> fieldSchemas, Map<String, Object> record) {
        return parseRecord(null, fieldSchemas, record);
    }

    private AbstractMap.SimpleEntry<Map<String, Object>, InterpretedFields> parseRecord(String recordId,
            Map<String, FieldSchema> fieldSchemas, Map<String, Object> record) {
        return parseRecord(null, fieldSchemas, record, null);
    }

    private AbstractMap.SimpleEntry<Map<String, Object>, InterpretedFields> parseRecord(String recordId,
            Map<String, FieldSchema> fieldSchemas, Map<String, Object> record, String modelId) {
        Map<String, Object> parsedRecord = new HashMap<String, Object>(record.size());
        parsedRecord.putAll(record);

        InterpretedFields interpretedFields = new InterpretedFields();

        List<String> extraFields = new ArrayList<>();
        Map<String, AbstractMap.SimpleEntry<Class<?>, Object>> mismatchedDataTypes = new HashMap<>();
        for (String fieldName : parsedRecord.keySet()) {
            if (!fieldSchemas.containsKey(fieldName)) {
                extraFields.add(fieldName);
                continue;
            }

            FieldSchema schema = fieldSchemas.get(fieldName);
            setFieldTypes(mismatchedDataTypes, parsedRecord, fieldName, schema);
            interpretFields(interpretedFields, fieldName, schema);
        }
        if (!extraFields.isEmpty()) {
            addWarning(WarningCode.EXTRA_FIELDS, recordId, extraFields, modelId);
            for (String extraField : extraFields) {
                parsedRecord.remove(extraField);
            }
        }
        if (!mismatchedDataTypes.isEmpty()) {
            throw new ScoringApiException(LedpCode.LEDP_31105,
                    new String[] { JsonUtils.serialize(mismatchedDataTypes) });
        }

        return new AbstractMap.SimpleEntry<Map<String, Object>, InterpretedFields>(parsedRecord, interpretedFields);
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
                FieldInterpretation.CompanyName, FieldInterpretation.State);
        expectedMatchFields.addAll(expectedDomainFields);
        boolean hasOneOfDomain = false;
        // PropData does not use City and will default Country to US
        boolean hasCompanyName = false;
        boolean hasCompanyState = false;

        for (String fieldName : fieldSchemas.keySet()) {
            FieldSchema fieldSchema = fieldSchemas.get(fieldName);

            if (fieldSchema.source != FieldSource.REQUEST) {
                continue;
            }
            if (!record.containsKey(fieldName)) {
                missingFields.add(fieldName);
            }

            Object fieldValue = record.get(fieldName);
            if (expectedMatchFields.contains(fieldSchema.interpretation)
                    && StringUtils.objectIsNullOrEmptyString(fieldValue)) {
                missingMatchFields.add(fieldName);
            }
            if (expectedDomainFields.contains(fieldSchema.interpretation)
                    && !StringUtils.objectIsNullOrEmptyString(fieldValue)) {
                hasOneOfDomain = true;
            }

            if (fieldSchema.interpretation == FieldInterpretation.CompanyName
                    && !StringUtils.objectIsNullOrEmptyString(fieldValue)) {
                hasCompanyName = true;
            } else if (fieldSchema.interpretation == FieldInterpretation.State
                    && !StringUtils.objectIsNullOrEmptyString(fieldValue)) {
                hasCompanyState = true;
            }
        }

        ScoringApiException missingEssentialFieldsException = modelJsonTypeHandler.checkForMissingEssentialFields(
                recordId, modelId, hasOneOfDomain, hasCompanyName, hasCompanyState, missingMatchFields);

        if (!missingFields.isEmpty()) {
            addWarning(WarningCode.MISSING_COLUMN, recordId, missingFields, modelId);
        }

        return missingEssentialFieldsException;
    }

    private Map<String, Object> transform(ScoringArtifacts scoringArtifacts, Map<String, Object> matchedRecord) {
        Map<String, Object> standardTransformedRecord = recordTransformer.transform(
                scoringArtifacts.getModelArtifactsDir().getAbsolutePath(),
                scoringArtifacts.getEventTableDataComposition().transforms, matchedRecord);

        Map<String, Object> datascienceTransformedRecord = recordTransformer.transform(
                scoringArtifacts.getModelArtifactsDir().getAbsolutePath(),
                scoringArtifacts.getDataScienceDataComposition().transforms, standardTransformedRecord);
        return datascienceTransformedRecord;
    }

    private void interpretFields(InterpretedFields interpretedFields, String fieldName, FieldSchema schema) {
        switch (schema.interpretation) {
        case Id:
            interpretedFields.setRecordId(fieldName);
            break;
        case Email:
            interpretedFields.setEmailAddress(fieldName);
            break;
        case Website:
            interpretedFields.setWebsite(fieldName);
            break;
        case CompanyName:
            interpretedFields.setCompanyName(fieldName);
            break;
        case City:
            interpretedFields.setCompanyCity(fieldName);
            break;
        case State:
            interpretedFields.setCompanyState(fieldName);
            break;
        case Country:
            interpretedFields.setCompanyCountry(fieldName);
            break;
        case Domain:
            interpretedFields.setDomain(fieldName);
            break;
        default:
            break;
        }
    }

    private void setFieldTypes(Map<String, AbstractMap.SimpleEntry<Class<?>, Object>> mismatchedDataTypes,
            Map<String, Object> record, String fieldName, FieldSchema schema) {
        Object fieldValue = record.get(fieldName);
        if (schema.source == FieldSource.REQUEST && fieldValue != null) {
            FieldType fieldType = schema.type;

            try {
                if (schema.interpretation == FieldInterpretation.Date) {
                    if (!StringUtils.objectIsNullOrEmptyString(fieldValue)) {
                        fieldValue = TimeStampConvertUtils.convertToLong(String.valueOf(fieldValue));
                    }
                } else {
                    fieldValue = FieldType.parse(fieldType, fieldValue);
                }
                record.put(fieldName, fieldValue);
            } catch (Exception e) {
                mismatchedDataTypes.put(fieldName,
                        new AbstractMap.SimpleEntry<Class<?>, Object>(fieldType.type(), fieldValue));
            }
        }
    }

    List<Warning> getWarnings(String recordId) {
        return warnings.getWarnings(recordId);
    }

    void addWarning(WarningCode code, String recordId, List<String> fields, String modelId) {
        warnings.addWarning(recordId,
                new Warning(code, new String[] { getWarningPrefix(modelId) + Joiner.on(",").join(fields) }));
    }

    String getWarningPrefix(String modelId) {
        return StringUtils.objectIsNullOrEmptyString(modelId) ? "" : "[For ModelId - " + modelId + "] => ";
    }
}
