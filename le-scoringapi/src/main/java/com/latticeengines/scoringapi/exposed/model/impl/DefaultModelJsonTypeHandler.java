package com.latticeengines.scoringapi.exposed.model.impl;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Joiner;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;
import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.DebugScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.Warning;
import com.latticeengines.domain.exposed.scoringapi.WarningCode;
import com.latticeengines.domain.exposed.scoringapi.Warnings;
import com.latticeengines.scoringapi.exposed.InterpretedFields;
import com.latticeengines.scoringapi.exposed.ScoreEvaluation;
import com.latticeengines.scoringapi.exposed.ScoreType;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;
import com.latticeengines.scoringapi.exposed.exception.ScoringApiException;
import com.latticeengines.scoringapi.exposed.model.ModelEvaluator;
import com.latticeengines.scoringapi.exposed.model.ModelJsonTypeHandler;

@Component
public class DefaultModelJsonTypeHandler implements ModelJsonTypeHandler {
    private static final Log log = LogFactory.getLog(DefaultModelJsonTypeHandler.class);

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    private Warnings warnings;

    private Map<String, FieldSchema> defaultFieldSchemaForMatch;

    @PostConstruct
    private void init() {
        defaultFieldSchemaForMatch = new HashMap<>();
        populateDefaultFieldSchemas();
    }

    private void populateDefaultFieldSchemas() {
        populateFieldSchema(FieldInterpretation.Domain);
        populateFieldSchema(FieldInterpretation.Email);
        populateFieldSchema(FieldInterpretation.Website);
        populateFieldSchema(FieldInterpretation.DUNS);
        populateFieldSchema(FieldInterpretation.City);
        populateFieldSchema(FieldInterpretation.CompanyName);
        populateFieldSchema(FieldInterpretation.Country);
        populateFieldSchema(FieldInterpretation.PhoneNumber);
        populateFieldSchema(FieldInterpretation.PostalCode);
        populateFieldSchema(FieldInterpretation.State);
        populateFieldSchema(FieldInterpretation.LatticeAccountID);
    }

    protected void populateFieldSchema(FieldInterpretation field) {
        FieldSchema fieldSchema = new FieldSchema(FieldSource.REQUEST, //
                FieldType.STRING, field);
        defaultFieldSchemaForMatch.put(field.toString(), fieldSchema);
    }

    @Override
    public boolean accept(String modelJsonType) {
        // anything other than PmmlModel, it future it will change if more types
        // are checked
        return !PMML_MODEL.equals(modelJsonType);
    }

    @Override
    public ModelEvaluator getModelEvaluator(String hdfsScoreArtifactBaseDir, //
            String modelJsonType, //
            String localPathToPersist) {
        FSDataInputStream is = null;
        String path = hdfsScoreArtifactBaseDir + PMML_FILENAME;

        ModelEvaluator modelEvaluator = null;
        try {
            FileSystem fs = FileSystem.newInstance(yarnConfiguration);
            is = fs.open(new Path(path));

            modelEvaluator = initModelEvaluator(is);

            if (!StringStandardizationUtils.objectIsNullOrEmptyString(localPathToPersist)) {
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, path, localPathToPersist + PMML_FILENAME);
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_31000, new String[] { path });
        }
        return modelEvaluator;
    }

    @Override
    public ScoreDerivation getScoreDerivation(String hdfsScoreArtifactBaseDir, //
            String modelJsonType, //
            String localPathToPersist) {
        String path = hdfsScoreArtifactBaseDir + HDFS_ENHANCEMENTS_DIR + SCORE_DERIVATION_FILENAME;
        String content = null;
        try {
            if (shouldStopCheckForScoreDerivation(path)) {
                return null;
            }

            content = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);
            if (!StringStandardizationUtils.objectIsNullOrEmptyString(localPathToPersist)) {
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, path, localPathToPersist + SCORE_DERIVATION_FILENAME);
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_31000, new String[] { path });
        }
        ScoreDerivation scoreDerivation = JsonUtils.deserialize(content, ScoreDerivation.class);
        return scoreDerivation;
    }

    @Override
    public DataComposition getDataScienceDataComposition(String hdfsScoreArtifactBaseDir, //
            String localPathToPersist) {
        String path = hdfsScoreArtifactBaseDir + HDFS_ENHANCEMENTS_DIR + DATA_COMPOSITION_FILENAME;
        String content = null;
        try {
            content = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);
            if (!StringStandardizationUtils.objectIsNullOrEmptyString(localPathToPersist)) {
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, path, localPathToPersist + DATA_COMPOSITION_FILENAME);
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_31000, new String[] { path });
        }
        DataComposition dataComposition = JsonUtils.deserialize(content, DataComposition.class);
        return dataComposition;
    }

    @Override
    public DataComposition getEventTableDataComposition(String hdfsScoreArtifactTableDirWithWildChar, //
            String localPathToPersist) {
        String path = null;
        String content = null;

        try {
            path = hdfsScoreArtifactTableDirWithWildChar;

            List<String> resolvedHdfsScoreArtifactTableDirs = HdfsUtils.getFilesByGlob(yarnConfiguration,
                    hdfsScoreArtifactTableDirWithWildChar);
            String resolvedHdfsScoreArtifactTableDir = null;
            if (resolvedHdfsScoreArtifactTableDirs.size() == 1) {
                resolvedHdfsScoreArtifactTableDir = resolvedHdfsScoreArtifactTableDirs.get(0);
            } else {
                for (String dir : resolvedHdfsScoreArtifactTableDirs) {

                    if (!hdfsScoreArtifactTableDirWithWildChar.equals(dir)) {
                        // pick first matching dir
                        resolvedHdfsScoreArtifactTableDir = dir;
                        break;
                    }
                }
            }

            if (!resolvedHdfsScoreArtifactTableDir.endsWith(PATH_SEPARATOR)) {
                resolvedHdfsScoreArtifactTableDir += PATH_SEPARATOR;
            }

            path = resolvedHdfsScoreArtifactTableDir + DATA_COMPOSITION_FILENAME;

            content = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);
            if (!StringStandardizationUtils.objectIsNullOrEmptyString(localPathToPersist)) {
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, path,
                        localPathToPersist + "metadata-" + DATA_COMPOSITION_FILENAME);
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_31000, new String[] { path });
        }
        DataComposition dataComposition = JsonUtils.deserialize(content, DataComposition.class);
        return dataComposition;
    }

    @Override
    public ScoreResponse generateScoreResponse(ScoringArtifacts scoringArtifacts, //
            Map<String, Object> transformedRecord) {
        ScoreResponse scoreResponse = new ScoreResponse();
        int percentile = score(scoringArtifacts, transformedRecord).getPercentile();
        BucketName bucket = score(scoringArtifacts, transformedRecord).getBucketName();
        scoreResponse.setScore(percentile);
        scoreResponse.setBucket(bucket);
        return scoreResponse;
    }

    @Override
    public DebugScoreResponse generateDebugScoreResponse(ScoringArtifacts scoringArtifacts, //
            Map<String, Object> transformedRecord, Map<String, Object> matchedRecord, //
            List<String> matchLogs, List<String> matchErrorLogs) {
        DebugScoreResponse debugScoreResponse = new DebugScoreResponse();
        ScoreEvaluation scoreEvaluation = score(scoringArtifacts, transformedRecord);
        debugScoreResponse.setProbability(scoreEvaluation.getProbabilityOrValue());
        debugScoreResponse.setScore(scoreEvaluation.getPercentile());
        debugScoreResponse.setBucket(scoreEvaluation.getBucketName());
        debugScoreResponse.setTransformedRecord(transformedRecord);
        debugScoreResponse.setMatchedRecord(matchedRecord);
        debugScoreResponse.setMatchLogs(matchLogs);
        debugScoreResponse.setMatchErrorMessages(matchErrorLogs);

        return debugScoreResponse;
    }

    @Override
    public ScoringApiException checkForMissingEssentialFields(String recordId, //
            String modelId, //
            boolean hasOneOfDomain, //
            boolean hasCompanyName, //
            boolean hasCompanyState, //
            List<String> missingMatchFields) {
        if (!hasOneOfDomain && (!hasCompanyName || !hasCompanyState)) {
            return new ScoringApiException(LedpCode.LEDP_31199,
                    new String[] { Joiner.on(",").join(missingMatchFields) });
        }

        return null;
    }

    @Override
    public AbstractMap.SimpleEntry<Map<String, Object>, InterpretedFields> parseRecord(String recordId,
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
            setFieldTypes(mismatchedDataTypes, parsedRecord, fieldName, schema,
                    shouldThrowExceptionForMismatchedDataTypes());
            interpretFields(interpretedFields, fieldName, schema);
        }

        for (String fieldName : defaultFieldSchemaForMatch.keySet()) {
            if (fieldSchemas.containsKey(fieldName)) {
                continue;
            } else {
                if (!parsedRecord.containsKey(fieldName)) {
                    continue;
                }

                if (extraFields.contains(fieldName)) {
                    extraFields.remove(fieldName);
                }
            }

            FieldSchema schema = defaultFieldSchemaForMatch.get(fieldName);
            interpretFields(interpretedFields, fieldName, schema);
        }

        if (!extraFields.isEmpty()) {
            addWarning(WarningCode.EXTRA_FIELDS, recordId, extraFields, modelId);
            for (String extraField : extraFields) {
                parsedRecord.remove(extraField);
            }
        }
        if (!mismatchedDataTypes.isEmpty()) {
            if (shouldThrowExceptionForMismatchedDataTypes()) {
                throw new ScoringApiException(LedpCode.LEDP_31105,
                        new String[] { JsonUtils.serialize(mismatchedDataTypes) });
            } else {
                List<String> warningMessages = new ArrayList<>();
                warningMessages.add(JsonUtils.serialize(mismatchedDataTypes));
                addWarning(WarningCode.MISMATCHED_DATATYPE, recordId, warningMessages, modelId);
            }
        }

        return new AbstractMap.SimpleEntry<Map<String, Object>, InterpretedFields>(parsedRecord, interpretedFields);
    }

    protected boolean shouldThrowExceptionForMismatchedDataTypes() {
        return false;
    }

    protected void handleException(Map<String, AbstractMap.SimpleEntry<Class<?>, Object>> mismatchedDataTypes,
            String fieldName, Object fieldValue, FieldType fieldType, Map<String, Object> record) {
        mismatchedDataTypes.put(fieldName, new AbstractMap.SimpleEntry<Class<?>, Object>(fieldType.type(), fieldValue));
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
        case PostalCode:
            interpretedFields.setPostalCode(fieldName);
            break;
        case PhoneNumber:
            interpretedFields.setPhoneNumber(fieldName);
            break;
        case DUNS:
            interpretedFields.setDuns(fieldName);
            break;
        case LatticeAccountID:
            interpretedFields.setLatticeAccountId(fieldName);
            break;
        default:
            break;
        }
    }

    private void setFieldTypes(Map<String, AbstractMap.SimpleEntry<Class<?>, Object>> mismatchedDataTypes,
            Map<String, Object> record, String fieldName, FieldSchema schema,
            boolean shouldThrowExceptionForMismatchedDataTypes) {
        Object fieldValue = record.get(fieldName);
        if (schema.source == FieldSource.REQUEST && fieldValue != null) {
            FieldType fieldType = schema.type;

            parseField(mismatchedDataTypes, record, fieldName, schema, fieldValue, fieldType,
                    shouldThrowExceptionForMismatchedDataTypes);
        }
    }

    private void parseField(Map<String, AbstractMap.SimpleEntry<Class<?>, Object>> mismatchedDataTypes,
            Map<String, Object> record, String fieldName, FieldSchema schema, Object fieldValue, FieldType fieldType,
            boolean shouldThrowExceptionForMismatchedDataTypes) {
        try {
            if (schema.interpretation == FieldInterpretation.Date) {
                if (!StringStandardizationUtils.objectIsNullOrEmptyString(fieldValue)) {
                    fieldValue = TimeStampConvertUtils.convertToLong(String.valueOf(fieldValue));
                }
            } else {
                fieldValue = FieldType.parse(fieldType, fieldValue);
            }
            record.put(fieldName, fieldValue);
        } catch (Exception e) {
            handleException(mismatchedDataTypes, fieldName, fieldValue, fieldType, record);
            if (!shouldThrowExceptionForMismatchedDataTypes) {
                record.put(fieldName, null);
            }
        }
    }

    private void addWarning(WarningCode code, String recordId, List<String> fields, String modelId) {
        warnings.addWarning(recordId,
                new Warning(code, new String[] { getWarningPrefix(modelId) + Joiner.on(",").join(fields) }));
    }

    private String getWarningPrefix(String modelId) {
        return StringStandardizationUtils.objectIsNullOrEmptyString(modelId) ? "" : "[For ModelId - " + modelId + "] => ";
    }

    protected boolean shouldStopCheckForScoreDerivation(String path) throws IOException {
        return false;
    }

    protected ModelEvaluator initModelEvaluator(FSDataInputStream is) {
        return new DefaultModelEvaluator(is);
    }

    protected ScoreEvaluation score(ScoringArtifacts scoringArtifacts, //
            Map<String, Object> transformedRecord) {
        Map<ScoreType, Object> evaluation = scoringArtifacts.getPmmlEvaluator().evaluate(transformedRecord,
                scoringArtifacts.getScoreDerivation());
        double probability = BigDecimal.valueOf((double) evaluation.get(ScoreType.PROBABILITY_OR_VALUE))
                .setScale(8, RoundingMode.HALF_UP).doubleValue();
        Object percentileObject = evaluation.get(ScoreType.PERCENTILE);

        int percentile = (int) percentileObject;
        if (percentile > 99 || percentile < 5) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Score out of range; percentile: %d probability: %,.7f", percentile,
                        (double) evaluation.get(ScoreType.PROBABILITY_OR_VALUE)));
            }
            percentile = Math.min(percentile, 99);
            percentile = Math.max(percentile, 5);
        }

        BucketName bucketName = bucketPercileScore(scoringArtifacts, percentile);
        return new ScoreEvaluation(probability, percentile, bucketName);
    }

    protected BucketName bucketPercileScore(ScoringArtifacts scoringArtifacts, int percentile) {
        BucketName bucketName = null;
        List<BucketMetadata> bucketMetadataList = scoringArtifacts.getBucketMetadataList();
        int min = 99;
        int max = 5;
        BucketName minBucket = null;
        BucketName maxBucket = null;
        boolean withinRange = false;
        if (bucketMetadataList != null && !bucketMetadataList.isEmpty()) {
            for (BucketMetadata bucketMetadata : bucketMetadataList) {
                int leftBoundScore = bucketMetadata.getLeftBoundScore();
                int rightBoundScore = bucketMetadata.getRightBoundScore();
                BucketName currentBucketName = bucketMetadata.getBucketName();
                if (leftBoundScore < min) {
                    min = leftBoundScore;
                    minBucket = currentBucketName;
                }
                if (rightBoundScore > max) {
                    max = rightBoundScore;
                    maxBucket = currentBucketName;
                }
                if (percentile > leftBoundScore && percentile <= rightBoundScore) {
                    withinRange = true;
                    bucketName = currentBucketName;
                }
            }
            if (min > max) {
                throw new RuntimeException("Bucket metadata has wrong buckets");
            }
            if (!withinRange && percentile == min) {
                withinRange = true;
                bucketName = minBucket;
            } else if (!withinRange && percentile < min) {
                log.warn(String.format("%d is less than minimum bound, setting to %s", percentile,
                        minBucket.toString()));
                bucketName = minBucket;
            } else if (!withinRange && percentile > max) {
                log.warn(String.format("%d is more than maximum bound, setting to %s", percentile,
                        maxBucket.toString()));
                bucketName = maxBucket;
            }
        }
        return bucketName;
    }
}
