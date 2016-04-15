package com.latticeengines.scoringapi.score.impl;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.scoringapi.controller.ScoreResource;
import com.latticeengines.scoringapi.exposed.DebugScoreResponse;
import com.latticeengines.scoringapi.exposed.InterpretedFields;
import com.latticeengines.scoringapi.exposed.ScoreEvaluation;
import com.latticeengines.scoringapi.exposed.ScoreRequest;
import com.latticeengines.scoringapi.exposed.ScoreResponse;
import com.latticeengines.scoringapi.exposed.ScoreType;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;
import com.latticeengines.scoringapi.exposed.context.RequestInfo;
import com.latticeengines.scoringapi.exposed.exception.ScoringApiException;
import com.latticeengines.scoringapi.exposed.model.ModelRetriever;
import com.latticeengines.scoringapi.exposed.warnings.Warning;
import com.latticeengines.scoringapi.exposed.warnings.WarningCode;
import com.latticeengines.scoringapi.exposed.warnings.Warnings;
import com.latticeengines.scoringapi.match.Matcher;
import com.latticeengines.scoringapi.score.ScoreRequestProcessor;
import com.latticeengines.scoringapi.transform.RecordTransformer;

@Component("scoreRequestProcessor")
public class ScoreRequestProcessorImpl implements ScoreRequestProcessor {

    private static final Log log = LogFactory.getLog(ScoreResource.class);

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

    private DateTimeFormatter timestampFormatter = ISODateTimeFormat.dateTime();

    @Override
    public ScoreResponse process(CustomerSpace space, ScoreRequest request, boolean isDebug) {
        split("requestPreparation");
        if (Strings.isNullOrEmpty(request.getModelId())) {
            throw new ScoringApiException(LedpCode.LEDP_31101);
        }

        requestInfo.put("Rule", Strings.nullToEmpty(request.getRule()));
        requestInfo.put("Source", Strings.nullToEmpty(request.getSource()));

        ScoringArtifacts scoringArtifacts = modelRetriever.getModelArtifacts(space, request.getModelId());
        requestInfo.put("ModelId", scoringArtifacts.getModelSummary().getId());
        requestInfo.put("ModelName", scoringArtifacts.getModelSummary().getName());
        requestInfo.put("ModelType", scoringArtifacts.getModelType().name());

        Map<String, FieldSchema> fieldSchemas = scoringArtifacts.getFieldSchemas();
        split("retrieveModelArtifacts");

        checkForMissingFields(fieldSchemas, request.getRecord());
        AbstractMap.SimpleEntry<Map<String, Object>, InterpretedFields> parsedRecordAndInterpretedFields = parseRecord(
                fieldSchemas, request.getRecord());
        String recordId = getIdIfAvailable(parsedRecordAndInterpretedFields.getValue(), request.getRecord());
        requestInfo.put("RecordId", recordId);
        split("parseRecord");

        Map<String, Object> matchedRecord = matcher.matchAndJoin(space, parsedRecordAndInterpretedFields.getValue(),
                fieldSchemas, parsedRecordAndInterpretedFields.getKey());
        addMissingFields(fieldSchemas, matchedRecord);
        split("matchRecord");

        Map<String, Object> transformedRecord = transform(scoringArtifacts, matchedRecord);
        split("transformRecord");

        ScoreResponse scoreResponse = null;
        if (isDebug) {
            scoreResponse = generateDebugScoreResponse(scoringArtifacts, transformedRecord, matchedRecord);
        } else {
            scoreResponse = generateScoreResponse(scoringArtifacts, transformedRecord);
        }
        scoreResponse.setId(recordId);
        scoreResponse.setTimestamp(timestampFormatter.print(DateTime.now(DateTimeZone.UTC)));
        split("scoreRecord");

        return scoreResponse;
    }

    private String getIdIfAvailable(InterpretedFields interpretedFields, Map<String, Object> record) {
        String value = "";
        if (!Strings.isNullOrEmpty(interpretedFields.getRecordId())) {
            Object id = record.get(interpretedFields.getRecordId());
            if (!StringUtils.objectIsNullOrEmptyString(id)) {
                value = String.valueOf(id);
            }
        }
        return value;
    }

    private void split(String key) {
        httpStopWatch.split(key);
    }

    private AbstractMap.SimpleEntry<Map<String, Object>, InterpretedFields> parseRecord(
            Map<String, FieldSchema> fieldSchemas, Map<String, Object> record) {
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
            warnings.addWarning(new Warning(WarningCode.EXTRA_FIELDS, new String[] { Joiner.on(",").join(extraFields) }));
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

    private void checkForMissingFields(Map<String, FieldSchema> fieldSchemas, Map<String, Object> record) {
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
        if (!hasOneOfDomain && (!hasCompanyName || !hasCompanyState)) {
            throw new ScoringApiException(LedpCode.LEDP_31199, new String[] { Joiner.on(",").join(missingMatchFields) });
        }
        if (!missingFields.isEmpty()) {
            warnings.addWarning(new Warning(WarningCode.MISSING_COLUMN, new String[] { Joiner.on(",").join(
                    missingFields) }));
        }
    }

    private ScoreResponse generateScoreResponse(ScoringArtifacts scoringArtifacts, Map<String, Object> transformedRecord) {
        ScoreResponse scoreResponse = new ScoreResponse();
        int percentile = score(scoringArtifacts, transformedRecord).getPercentile();
        scoreResponse.setScore(percentile);
        return scoreResponse;
    }

    private DebugScoreResponse generateDebugScoreResponse(ScoringArtifacts scoringArtifacts,
            Map<String, Object> transformedRecord, Map<String, Object> matchedRecord) {
        DebugScoreResponse debugScoreResponse = new DebugScoreResponse();
        ScoreEvaluation scoreEvaluation = score(scoringArtifacts, transformedRecord);
        debugScoreResponse.setProbability(scoreEvaluation.getProbability());
        debugScoreResponse.setScore(scoreEvaluation.getPercentile());
        debugScoreResponse.setTransformedRecord(transformedRecord);
        debugScoreResponse.setMatchedRecord(matchedRecord);

        return debugScoreResponse;
    }

    private ScoreEvaluation score(ScoringArtifacts scoringArtifacts, Map<String, Object> transformedRecord) {
        Map<ScoreType, Object> evaluation = scoringArtifacts.getPmmlEvaluator().evaluate(transformedRecord,
                scoringArtifacts.getScoreDerivation());
        double probability = (double) evaluation.get(ScoreType.PROBABILITY);
        Object percentileObject = evaluation.get(ScoreType.PERCENTILE);

        int percentile = (int) percentileObject;
        if (percentile > 99 || percentile < 5) {
            log.warn(String.format("Score out of range; percentile: %d probability: %,.7f", percentile,
                    (double) evaluation.get(ScoreType.PROBABILITY)));
            percentile = Math.min(percentile, 99);
            percentile = Math.max(percentile, 5);
        }

        return new ScoreEvaluation(probability, percentile);
    }

    private Map<String, Object> transform(ScoringArtifacts scoringArtifacts, Map<String, Object> matchedRecord) {
        Map<String, Object> standardTransformedRecord = recordTransformer.transform(scoringArtifacts
                .getModelArtifactsDir().getAbsolutePath(), scoringArtifacts.getEventTableDataComposition().transforms,
                matchedRecord);

        Map<String, Object> datascienceTransformedRecord = recordTransformer.transform(scoringArtifacts
                .getModelArtifactsDir().getAbsolutePath(), scoringArtifacts.getDataScienceDataComposition().transforms,
                standardTransformedRecord);
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
                mismatchedDataTypes.put(fieldName, new AbstractMap.SimpleEntry<Class<?>, Object>(fieldType.type(),
                        fieldValue));
            }
        }
    }

}
