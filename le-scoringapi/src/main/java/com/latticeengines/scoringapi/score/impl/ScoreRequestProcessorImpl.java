package com.latticeengines.scoringapi.score.impl;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.latticeengines.common.exposed.rest.HttpStopWatch;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.scoringapi.controller.ScoreResource;
import com.latticeengines.scoringapi.exception.ScoringApiException;
import com.latticeengines.scoringapi.exposed.InterpretedFields;
import com.latticeengines.scoringapi.exposed.ScoreRequest;
import com.latticeengines.scoringapi.exposed.ScoreResponse;
import com.latticeengines.scoringapi.exposed.ScoreType;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;
import com.latticeengines.scoringapi.match.Matcher;
import com.latticeengines.scoringapi.model.ModelRetriever;
import com.latticeengines.scoringapi.score.ScoreRequestProcessor;
import com.latticeengines.scoringapi.transform.RecordTransformer;
import com.latticeengines.scoringapi.warnings.Warning;
import com.latticeengines.scoringapi.warnings.WarningCode;
import com.latticeengines.scoringapi.warnings.Warnings;

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

    @Override
    public ScoreResponse process(CustomerSpace space, ScoreRequest request, ScoreType scoreType) {
        log.info(String.format("{'requestPreparationDuration':%sms}", httpStopWatch.splitAndGetTimeSinceLastSplit()));
        if (Strings.isNullOrEmpty(request.getModelId())) {
            throw new ScoringApiException(LedpCode.LEDP_31101);
        }

        ScoringArtifacts scoringArtifacts = modelRetriever.getModelArtifacts(space, request.getModelId());
        Map<String, FieldSchema> fieldSchemas = scoringArtifacts.getDataComposition().fields;
        log.info(String.format("{'retrieveModelArtifacts':%sms}", httpStopWatch.splitAndGetTimeSinceLastSplit()));

        checkForMissingFields(fieldSchemas, request.getRecord());
        InterpretedFields interpretedFields = examineRecord(fieldSchemas, request.getRecord());
        log.info(String.format("{'inspectRecord':%sms}", httpStopWatch.splitAndGetTimeSinceLastSplit()));

        Map<String, Object> matchedRecord = matcher.matchAndJoin(space, interpretedFields, fieldSchemas,
                request.getRecord());
        log.info(String.format("{'matchRecord':%sms}", httpStopWatch.splitAndGetTimeSinceLastSplit()));

        Map<String, Object> transformedRecord = transform(scoringArtifacts, matchedRecord);
        log.info(String.format("{'transformRecord':%sms}", httpStopWatch.splitAndGetTimeSinceLastSplit()));

        double score = score(scoringArtifacts, transformedRecord, scoreType);
        log.info(String.format("{'scoreRecord':%sms}", httpStopWatch.splitAndGetTimeSinceLastSplit()));

        ScoreResponse scoreResponse = new ScoreResponse();
        scoreResponse.setScore(score);
        return scoreResponse;
    }

    private InterpretedFields examineRecord(Map<String, FieldSchema> fieldSchemas, Map<String, Object> record) {
        InterpretedFields interpretedFields = new InterpretedFields();

        List<String> extraFields = new ArrayList<>();
        Map<String, AbstractMap.SimpleEntry<Class<?>, Object>> mismatchedDataTypes = new HashMap<>();
        for (String fieldName : record.keySet()) {
            if (!fieldSchemas.containsKey(fieldName)) {
                extraFields.add(fieldName);
                continue;
            }

            FieldSchema schema = fieldSchemas.get(fieldName);
            Object fieldValue = record.get(fieldName);
            if (schema.source == FieldSource.REQUEST && fieldValue != null) {
                Class<?> typeClass = schema.type.type();
                if (!typeClass.isInstance(fieldValue)) {
                    mismatchedDataTypes.put(fieldName, new AbstractMap.SimpleEntry<Class<?>, Object>(typeClass,
                            fieldValue));
                }
            }

            switch (schema.interpretation) {
            case ID:
                interpretedFields.setRecordId(fieldName);
                break;
            case EMAIL_ADDRESS:
                interpretedFields.setEmailAddress(fieldName);
                break;
            case WEBSITE:
                interpretedFields.setWebsite(fieldName);
                break;
            case COMPANY_NAME:
                interpretedFields.setCompanyName(fieldName);
                break;
            case COMPANY_CITY:
                interpretedFields.setCompanyCity(fieldName);
                break;
            case COMPANY_STATE:
                interpretedFields.setCompanyState(fieldName);
                break;
            case COMPANY_COUNTRY:
                interpretedFields.setCompanyCountry(fieldName);
                break;
            case DOMAIN:
                interpretedFields.setDomain(fieldName);
                break;
            default:
                break;
            }
        }
        if (!extraFields.isEmpty()) {
            warnings.addWarning(new Warning(WarningCode.EXTRA_FIELDS, new String[] { Joiner.on(",").join(extraFields) }));
            for (String extraField : extraFields) {
                record.remove(extraField);
            }
        }
        if (!mismatchedDataTypes.isEmpty()) {
            warnings.addWarning(new Warning(WarningCode.MISMATCHED_DATATYPE, new String[] { JsonUtils
                    .serialize(mismatchedDataTypes) }));
        }

        return interpretedFields;
    }

    private void checkForMissingFields(Map<String, FieldSchema> fieldSchemas, Map<String, Object> record) {
        List<String> missingMatchFields = new ArrayList<>();
        List<String> missingFields = new ArrayList<>();

        EnumSet<FieldInterpretation> expectedDomainFields = EnumSet.of(FieldInterpretation.DOMAIN,
                FieldInterpretation.EMAIL_ADDRESS, FieldInterpretation.WEBSITE);
        EnumSet<FieldInterpretation> expectedMatchFields = EnumSet.of(FieldInterpretation.COMPANY_COUNTRY,
                FieldInterpretation.COMPANY_NAME, FieldInterpretation.COMPANY_STATE);
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

            if (expectedMatchFields.contains(fieldSchema.interpretation) && !record.containsKey(fieldName)) {
                missingMatchFields.add(fieldName);
            }
            if (expectedDomainFields.contains(fieldSchema.interpretation) && record.containsKey(fieldName)) {
                hasOneOfDomain = true;
            }

            if (fieldSchema.interpretation == FieldInterpretation.COMPANY_NAME && record.containsKey(fieldName)) {
                hasCompanyName = true;
            } else if (fieldSchema.interpretation == FieldInterpretation.COMPANY_STATE && record.containsKey(fieldName)) {
                hasCompanyState = true;
            }
        }
        if (!hasOneOfDomain && (!hasCompanyName && !hasCompanyState)) {
            throw new ScoringApiException(LedpCode.LEDP_31199, new String[] { Joiner.on(",").join(missingMatchFields) });
        }
        if (!missingFields.isEmpty()) {
            warnings.addWarning(new Warning(WarningCode.MISSING_COLUMN, new String[] { Joiner.on(",").join(
                    missingFields) }));
        }
    }

    private double score(ScoringArtifacts scoringArtifacts, Map<String, Object> transformedRecord, ScoreType scoreType) {
        Map<ScoreType, Object> evaluation = scoringArtifacts.getPmmlEvaluator().evaluate(transformedRecord,
                scoringArtifacts.getScoreDerivation(), scoringArtifacts.getDataComposition().fields);
        double score = (double) evaluation.get(ScoreType.PROBABILITY);
        if (scoreType == ScoreType.PERCENTILE) {
            Object percentileObject = evaluation.get(ScoreType.PERCENTILE);
            if (percentileObject == null) {
                throw new LedpException(LedpCode.LEDP_31011, new String[] {
                        String.valueOf(evaluation.get(ScoreType.PROBABILITY)), scoringArtifacts.getModelId() });
            }

            int percentile = (int) percentileObject;
            if (percentile > 99 || percentile < 5) {
                log.warn(String.format("Score out of range; percentile: %d probability: %,.7f", percentile,
                        (double) evaluation.get(ScoreType.PROBABILITY)));
                percentile = Math.min(percentile, 99);
                percentile = Math.max(percentile, 5);
            }
            score = percentile;
        }

        return score;
    }

    private Map<String, Object> transform(ScoringArtifacts scoringArtifacts, Map<String, Object> matchedRecord) {
        Map<String, Object> standardTransformedRecord = recordTransformer.transform(scoringArtifacts
                .getModelArtifactsDir().getAbsolutePath(), scoringArtifacts.getMetadataDataComposition().transforms,
                matchedRecord);

        Map<String, Object> datascienceTransformedRecord = recordTransformer.transform(scoringArtifacts
                .getModelArtifactsDir().getAbsolutePath(), scoringArtifacts.getDataComposition().transforms,
                standardTransformedRecord);
        return datascienceTransformedRecord;
    }

}
