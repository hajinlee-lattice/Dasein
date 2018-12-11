package com.latticeengines.scoringapi.exposed.model.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.jpmml.evaluator.CacheUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.base.Joiner;
import com.google.common.cache.CacheBuilderSpec;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PrecisionUtils;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;
import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
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
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.scoringapi.exposed.InterpretedFields;
import com.latticeengines.scoringapi.exposed.ScoreEvaluation;
import com.latticeengines.scoringapi.exposed.ScoreType;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;
import com.latticeengines.scoringapi.exposed.exception.ScoringApiException;
import com.latticeengines.scoringapi.exposed.model.ModelEvaluator;
import com.latticeengines.scoringapi.exposed.model.ModelJsonTypeHandler;

@Component
public class DefaultModelJsonTypeHandler implements ModelJsonTypeHandler {
    private static final Logger log = LoggerFactory.getLogger(DefaultModelJsonTypeHandler.class);

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    private Warnings warnings;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    @Value("${camille.zk.pod.id:Default}")
    private String podId;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    private Map<String, FieldSchema> defaultFieldSchemaForMatch;

    static {
        CacheUtil.setCacheBuilderSpec(CacheBuilderSpec.parse("weakKeys,weakValues"));
    }

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
        populateFieldSchema(FieldInterpretation.LatticeAccountId);
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
        String path = hdfsScoreArtifactBaseDir + PMML_FILENAME;

        ModelEvaluator modelEvaluator = null;
        try {
            path = getS3PathIfNeeded(path, false);
            modelEvaluator = initModelEvaluator(HdfsUtils.getInputStream(yarnConfiguration, path));

            if (!StringStandardizationUtils.objectIsNullOrEmptyString(localPathToPersist)) {
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, path, localPathToPersist + PMML_FILENAME);
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_31000, new String[] { path });
        }
        return modelEvaluator;
    }

    private String getS3PathIfNeeded(String hdfsPath, boolean isGlob) {
        String protocol = Boolean.TRUE.equals(useEmr) ? "s3a" : "s3n";
        HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder(protocol);
        return pathBuilder.getS3PathWithGlob(yarnConfiguration, hdfsPath, isGlob, podId, s3Bucket);
    }

    @Override
    public ScoreDerivation getScoreDerivation(String hdfsScoreArtifactBaseDir, //
            String modelJsonType, //
            String localPathToPersist) {
        String path = hdfsScoreArtifactBaseDir + HDFS_ENHANCEMENTS_DIR + SCORE_DERIVATION_FILENAME;
        String content = null;
        try {
            path = getS3PathIfNeeded(path, false);
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
            path = getS3PathIfNeeded(path, false);
            content = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);
            if (!StringStandardizationUtils.objectIsNullOrEmptyString(localPathToPersist)) {
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, path, localPathToPersist + DATA_COMPOSITION_FILENAME);
            }
        } catch (IOException e) {
            log.error("Failed to get DataComposition file, error=", e.getMessage());
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
            hdfsScoreArtifactTableDirWithWildChar = getS3PathIfNeeded(hdfsScoreArtifactTableDirWithWildChar, true);
            log.info("Event table composition dir=" + hdfsScoreArtifactTableDirWithWildChar);
            List<String> resolvedHdfsScoreArtifactTableDirs = HdfsUtils.getFilesByGlob(yarnConfiguration,
                    hdfsScoreArtifactTableDirWithWildChar);
            resolvedHdfsScoreArtifactTableDirs = new HdfsToS3PathBuilder()
                    .toHdfsPaths(resolvedHdfsScoreArtifactTableDirs);
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
            path = getS3PathIfNeeded(path, false);
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
            Map<String, Object> transformedRecord, boolean isCalledViaInternalResource) {
        ScoreResponse scoreResponse = isCalledViaInternalResource ? new DebugScoreResponse() : new ScoreResponse();
        ScoreEvaluation scoreEvaluation = score(scoringArtifacts, transformedRecord);
        scoreResponse.setScore(scoreEvaluation.getPercentile());
        scoreResponse
                .setBucket(scoreEvaluation.getBucketName() == null ? null : scoreEvaluation.getBucketName().toValue());

        // PLS-7570 - we need to make sure to always return probability score if
        // call was made via internal resource
        if (isCalledViaInternalResource) {
            ((DebugScoreResponse) scoreResponse).setProbability(scoreEvaluation.getProbabilityOrValue());
        }
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
        debugScoreResponse
                .setBucket(scoreEvaluation.getBucketName() == null ? null : scoreEvaluation.getBucketName().toValue());
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
            boolean hasKeyFieldForMatching, //
            List<String> missingMatchFields) {
        if (!hasOneOfDomain && !hasKeyFieldForMatching) {
            return new ScoringApiException(LedpCode.LEDP_31199,
                    new String[] { Joiner.on(",").join(missingMatchFields) });
        }

        return null;
    }

    @Override
    public AbstractMap.SimpleEntry<Map<String, Object>, InterpretedFields> parseRecord(String recordId,
            Map<String, FieldSchema> fieldSchemas, Map<String, Object> record, String modelId) {
        Map<String, Object> parsedRecord = new HashMap<String, Object>(record.size());

        handleEmptyString(record, parsedRecord);

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

    void handleEmptyString(Map<String, Object> record, Map<String, Object> parsedRecord) {
        // PLS-4058 - make sure to replace empty string value with null
        for (String key : record.keySet()) {
            Object value = record.get(key);
            if (value != null && value instanceof String) {
                CharSequence cs = ((String) value);
                if (StringUtils.isBlank(cs)) {
                    value = null;
                }
            }
            parsedRecord.put(key, value);
        }
    }

    @Override
    public Map<String, FieldSchema> getDefaultFieldSchemaForMatch() {
        return defaultFieldSchemaForMatch;
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
        case LatticeAccountId:
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
        return StringStandardizationUtils.objectIsNullOrEmptyString(modelId) ? ""
                : "[For ModelId - " + modelId + "] => ";
    }

    protected boolean shouldStopCheckForScoreDerivation(String path) throws IOException {
        return false;
    }

    protected ModelEvaluator initModelEvaluator(InputStream is) {
        return new DefaultModelEvaluator(is);
    }

    protected ScoreEvaluation score(ScoringArtifacts scoringArtifacts, //
            Map<String, Object> transformedRecord) {
        Map<ScoreType, Object> evaluation = scoringArtifacts.getPmmlEvaluator().evaluate(transformedRecord,
                scoringArtifacts.getScoreDerivation());
        double probability = PrecisionUtils.setPrecision((double) evaluation.get(ScoreType.PROBABILITY_OR_VALUE), 6);
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

    @SuppressWarnings("deprecation")
    protected BucketName bucketPercileScore(ScoringArtifacts scoringArtifacts, int percentile) {
        List<BucketMetadata> bucketMetadataList = scoringArtifacts.getBucketMetadataList();
        BucketName bucketName = null;
        int min = BucketName.A.getDefaultUpperBound();
        int max = BucketName.D.getDefaultLowerBound();
        BucketName minBucket = null;
        BucketName maxBucket = null;
        boolean withinRange = false;
        if (bucketMetadataList != null && !bucketMetadataList.isEmpty()) {
            for (BucketMetadata bucketMetadata : bucketMetadataList) {
                // leftBoundScore is the upper bound, and the rightBoundScore is
                // the lower bound
                int leftBoundScore = bucketMetadata.getLeftBoundScore();
                int rightBoundScore = bucketMetadata.getRightBoundScore();
                BucketName currentBucketName = bucketMetadata.getBucket();
                if (rightBoundScore < min) {
                    min = rightBoundScore;
                    minBucket = currentBucketName;
                }
                if (leftBoundScore > max) {
                    max = leftBoundScore;
                    maxBucket = currentBucketName;
                }
                if (percentile >= rightBoundScore && percentile <= leftBoundScore) {
                    withinRange = true;
                    bucketName = currentBucketName;
                }
            }
            if (log.isDebugEnabled()) {
                log.debug(String.format("min: %d, manx: %d", min, max));
            }
            if (min > max) {
                throw new RuntimeException("Bucket metadata has wrong buckets");
            }

            if (!withinRange && percentile < min) {
                log.warn(String.format("%d is less than minimum bound, setting to %s", percentile,
                        minBucket.toString()));
                bucketName = minBucket;
            } else if (!withinRange && percentile > max) {
                log.warn(String.format("%d is more than maximum bound, setting to %s", percentile,
                        maxBucket.toString()));
                bucketName = maxBucket;
            }
        } else {
            // use default bucketing criteria
            if (log.isDebugEnabled()) {
                log.debug("No bucket metadata is defined, therefore use default bucketing criteria.");
            }
            if (percentile < BucketName.D.getDefaultLowerBound()) {
                log.warn(
                        String.format("%d is less than minimum bound, setting to %s", percentile, BucketName.D.name()));
                bucketName = BucketName.D;
            } else if (percentile < BucketName.C.getDefaultLowerBound()) {
                bucketName = BucketName.D;
            } else if (percentile < BucketName.B.getDefaultLowerBound()) {
                bucketName = BucketName.C;
            } else if (percentile < BucketName.A.getDefaultLowerBound()) {
                bucketName = BucketName.B;
            } else if (percentile <= BucketName.A.getDefaultUpperBound()) {
                bucketName = BucketName.A;
            } else {
                log.warn(
                        String.format("%d is more than maximum bound, setting to %s", percentile, BucketName.A.name()));
                bucketName = BucketName.A;
            }
        }
        return bucketName;
    }
}
