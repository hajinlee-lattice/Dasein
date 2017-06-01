package com.latticeengines.scoringapi.exposed.model.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.camille.exposed.featureflags.FeatureFlagClient;
import com.latticeengines.common.exposed.modeling.ModelExtractor;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NetworkUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.Field;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretationCollections;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.domain.exposed.scoringapi.Fields;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.scoringapi.ModelDetail;
import com.latticeengines.domain.exposed.scoringapi.ModelType;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.scoringapi.exposed.ScoreCorrectnessArtifacts;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;
import com.latticeengines.scoringapi.exposed.exception.ScoringApiException;
import com.latticeengines.scoringapi.exposed.model.ModelEvaluator;
import com.latticeengines.scoringapi.exposed.model.ModelJsonTypeHandler;
import com.latticeengines.scoringapi.exposed.model.ModelRetriever;
import com.latticeengines.scoringinternalapi.controller.BaseScoring;

@Component("modelRetriever")
public class ModelRetrieverImpl implements ModelRetriever {

    private static final String DELIM = ":";
    private static final String MODEL_TYPE_KEY = "__type";
    private static final String MODEL_KEY = "Model";
    private static final Log log = LogFactory.getLog(ModelRetrieverImpl.class);
    private static final String UUID_IDENTIFIER = UUID.randomUUID().toString();
    public static final String HDFS_SCORE_ARTIFACT_EVENTTABLE_DIR = "/user/s-analytics/customers/%s/data/%s-*-Metadata/";
    public static final String HDFS_SCORE_ARTIFACT_APPID_DIR = "/user/s-analytics/customers/%s/models/%s/%s/";
    public static final String HDFS_SCORE_ARTIFACT_BASE_DIR = HDFS_SCORE_ARTIFACT_APPID_DIR + "%s/";
    public static final String MODEL_JSON_SUFFIX = "_model.json";
    public static final String MODEL_JSON = "model.json";
    public static final String DATA_EXPORT_CSV = "_dataexport.csv";
    public static final String SAMPLES_AVRO_PATH = "/user/s-analytics/customers/%s/data/%s/samples/";
    public static final String SCORED_TXT = "_scored.txt";
    public static final String RTS_DATA_CLOUD_VERSION = "1.0";
    private static final String STPIPELINE_BINARY = "STPipelineBinary.p";

    @VisibleForTesting
    static final String LOCAL_MODEL_ARTIFACT_CACHE_DIR = "artifacts/";

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    @Value("${scoringapi.scoreartifact.cache.maxsize}")
    private int scoreArtifactCacheMaxSize;

    @Value("${scoringapi.scoreartifact.cache.expiration.time}")
    private int scoreArtifactCacheExpirationTime;

    @Value("${scoringapi.scoreartifact.cache.refresh.time:120}")
    private int scoreArtifactCacheRefreshTime;

    @Autowired
    @Qualifier("commonTaskScheduler")
    private ThreadPoolTaskScheduler taskScheduler;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private List<ModelJsonTypeHandler> modelJsonTypeHandlers;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    private LoadingCache<AbstractMap.SimpleEntry<CustomerSpace, String>, ScoringArtifacts> scoreArtifactCache;

    private String localPathToPersist = null;

    @Value("${scoringapi.modeljson.cache.dir}")
    private String localModelJsonCacheDirProperty;

    private String localModelJsonCacheDirIdentifier;

    @VisibleForTesting
    void setScoreArtifactCacheMaxSize(int scoreArtifactCacheMaxSize) {
        this.scoreArtifactCacheMaxSize = scoreArtifactCacheMaxSize;
    }

    @VisibleForTesting
    void setScoreArtifactCacheExpirationTime(int scoreArtifactCacheExpirationTime) {
        this.scoreArtifactCacheExpirationTime = scoreArtifactCacheExpirationTime;
    }

    @VisibleForTesting
    void setScoreArtifactCacheRefreshTime(int scoreArtifactCacheRefreshTime) {
        this.scoreArtifactCacheRefreshTime = scoreArtifactCacheRefreshTime;
    }

    @VisibleForTesting
    void setTaskScheduler(ThreadPoolTaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
    }

    @PostConstruct
    public void initialize() throws Exception {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
        localModelJsonCacheDirIdentifier = StringUtils.defaultIfBlank(NetworkUtils.getHostName(), UUID_IDENTIFIER);
        instantiateCache();
        scheduleRefreshJob();
    }

    @Override
    public List<Model> getActiveModels(CustomerSpace customerSpace, ModelType type) {
        List<Model> models = new ArrayList<>();

        List<?> modelSummaries = internalResourceRestApiProxy.getActiveModelSummaries(customerSpace);
        convertModelSummaryToModel(type, models, modelSummaries, customerSpace);
        return models;
    }

    private void convertModelSummaryToModel(ModelType type, List<Model> models, List<?> modelSummaries,
            CustomerSpace customerSpace) {
        if (modelSummaries != null) {
            for (Object modelSummary : modelSummaries) {
                @SuppressWarnings("unchecked")
                Map<String, String> map = (Map<String, String>) modelSummary;
                ModelType modelType = getModelType(map.get("SourceSchemaInterpretation"));

                if ((type == ModelType.ACCOUNT && modelType == ModelType.ACCOUNT)
                        || (type == ModelType.CONTACT && modelType == ModelType.CONTACT)) {
                    Model model = new Model(map.get("Id"), map.get("DisplayName"), type);
                    models.add(model);
                }
            }
        }
    }

    private ModelType getModelType(String sourceSchemaInterpretation) {
        if (sourceSchemaInterpretation == null) {
            return null;
        } else if (sourceSchemaInterpretation.equals(SchemaInterpretation.SalesforceLead.name())) {
            return ModelType.CONTACT;
        } else if (sourceSchemaInterpretation.equals(SchemaInterpretation.SalesforceAccount.name())) {
            return ModelType.ACCOUNT;
        }
        return null;
    }

    @Override
    public Fields getModelFields(CustomerSpace customerSpace, String modelId) {
        return getModelFields(customerSpace, modelId, null);
    }

    @VisibleForTesting
    LoadingCache<AbstractMap.SimpleEntry<CustomerSpace, String>, ScoringArtifacts> getScoreArtifactCache() {
        return scoreArtifactCache;
    }

    @Override
    public Fields getModelFields(CustomerSpace customerSpace, String modelId, List<Predictor> predictors) {
        Fields fields = new Fields();
        fields.setModelId(modelId);
        List<Field> fieldList = new ArrayList<>();
        fields.setFields(fieldList);

        ScoringArtifacts artifacts = getModelArtifacts(customerSpace, modelId);

        Map<String, FieldSchema> mapFields = artifacts.getFieldSchemas();
        ModelSummary modelSummary = artifacts.getModelSummary();
        boolean fuzzyMatchEnabled = FeatureFlagClient.isEnabled(customerSpace,
                LatticeFeatureFlag.ENABLE_FUZZY_MATCH.getName());
        String dataCloudVersion = modelSummary.getDataCloudVersion();
        if (StringUtils.isEmpty(dataCloudVersion) || dataCloudVersion.equals(RTS_DATA_CLOUD_VERSION)) {
            fields.setValidationExpression(FieldInterpretationCollections.RTS_MODEL_VALIDATION_EXPRESSION);
        } else {
            if (fuzzyMatchEnabled) {
                fields.setValidationExpression(FieldInterpretationCollections.FUZZY_MATCH_VALIDATION_EXPRESSION);
            } else {
                fields.setValidationExpression(FieldInterpretationCollections.NON_FUZZY_MATCH_VALIDATION_EXPRESSION);
            }
        }

        if (!CollectionUtils.isEmpty(predictors)) {
            for (String fieldName : mapFields.keySet()) {
                String displayName = null;

                FieldSchema fieldSchema = mapFields.get(fieldName);

                if (fieldSchema.source.equals(FieldSource.REQUEST)) {
                    // use predictors to just get display name
                    for (Predictor predictor : predictors) {
                        if (fieldName.equals(predictor.getName())) {
                            // break loop as soon as we got matching predictor
                            displayName = predictor.getDisplayName();
                            break;
                        }
                    }

                    setField(fieldList, fieldName, displayName, fieldSchema);
                }
            }
        } else {
            // this is a fallback mechanism and tried to load field info via
            // table if modelSummary metadata does not have predictors
            Table modelMetadataTable = metadataProxy.getTable(customerSpace.toString(),
                    StringUtils.capitalize(StringUtils.lowerCase(artifacts.getModelSummary().getEventTableName())));
            Map<String, Attribute> attributeMap = null;
            if (modelMetadataTable != null) {
                attributeMap = modelMetadataTable.getNameAttributeMap();
            }
            for (String fieldName : mapFields.keySet()) {
                String displayName = null;

                FieldSchema fieldSchema = mapFields.get(fieldName);

                if (fieldSchema.source.equals(FieldSource.REQUEST)) {
                    if (attributeMap != null && attributeMap.containsKey(fieldName)) {
                        displayName = attributeMap.get(fieldName).getDisplayName();
                    }

                    setField(fieldList, fieldName, displayName, fieldSchema);
                }
            }
        }

        return fields;
    }

    @VisibleForTesting
    void setField(List<Field> fieldList, String fieldName, String displayName, FieldSchema fieldSchema) {
        if (StringUtils.isEmpty(displayName)) {
            // by default use field name as display name
            displayName = fieldName;
        }

        Field field = new Field(fieldName, fieldSchema.type, displayName);
        // Mark Fuzzylogic fields as primary fields to enforce model field
        // mapping
        FieldInterpretation keyField = null;
        try {
            keyField = FieldInterpretation.valueOf(fieldName);
            if (FieldInterpretationCollections.PrimaryMatchingFields.contains(keyField)) {
                field.setPrimaryField(true);
            }
        } catch (Exception e) {
            // Ignore. As this field doesn't belong to FieldInterpretation enum.
        }
        fieldList.add(field);
    }

    @VisibleForTesting
    ModelSummary getModelSummary(CustomerSpace customerSpace, String modelId) {
        ModelSummary modelSummary = internalResourceRestApiProxy.getModelSummaryFromModelId(modelId, customerSpace);
        if (modelSummary == null) {
            throw new ScoringApiException(LedpCode.LEDP_31102, new String[] { modelId });
        } else if (StringUtils.isBlank(modelSummary.getEventTableName())) {
            throw new LedpException(LedpCode.LEDP_31008, new String[] { modelId });
        }
        return modelSummary;
    }

    @VisibleForTesting
    List<BucketMetadata> getBucketMetadata(CustomerSpace customerSpace, String modelId) {
        List<BucketMetadata> bucketMetadataList = internalResourceRestApiProxy.getUpToDateABCDBuckets(modelId,
                customerSpace);
        if (bucketMetadataList == null) {
            throw new LedpException(LedpCode.LEDP_31200, new String[] { modelId });
        }
        return bucketMetadataList;
    }

    @VisibleForTesting
    List<ModelSummary> getModelSummariesModifiedWithinTimeFrame(long timeFrame) {
        List<ModelSummary> modelSummaryList = internalResourceRestApiProxy
                .getModelSummariesModifiedWithinTimeFrame(timeFrame);
        return modelSummaryList;
    }

    private Triple<String, String, String> determineScoreArtifactBaseEventTableAndSamplePath(
            CustomerSpace customerSpace, ModelSummary modelSummary) {
        AbstractMap.SimpleEntry<String, String> modelNameAndVersion = parseModelNameAndVersion(modelSummary);
        String appId = getModelAppIdSubfolder(customerSpace, modelSummary);

        String hdfsScoreArtifactBaseDir = String.format(HDFS_SCORE_ARTIFACT_BASE_DIR, customerSpace.toString(),
                modelNameAndVersion.getKey(), modelNameAndVersion.getValue(), appId);

        String hdfsScoreArtifactTableDir = String.format(HDFS_SCORE_ARTIFACT_EVENTTABLE_DIR, customerSpace.toString(),
                modelSummary.getEventTableName());

        String hdfsSamplesAvroPath = String.format(SAMPLES_AVRO_PATH, customerSpace.toString(),
                modelSummary.getEventTableName());

        return Triple.of(hdfsScoreArtifactBaseDir, hdfsScoreArtifactTableDir, hdfsSamplesAvroPath);
    }

    @Override
    public ScoringArtifacts retrieveModelArtifactsFromHdfs(CustomerSpace customerSpace, //
            String modelId) {
        log.info(String.format("Retrieving model artifacts from HDFS for model:%s", modelId));
        ModelSummary modelSummary = getModelSummary(customerSpace, modelId);
        List<BucketMetadata> bucketMetadataList = getBucketMetadata(customerSpace, modelId);
        ModelType modelType = getModelType(modelSummary.getSourceSchemaInterpretation());
        Triple<String, String, String> artifactBaseAndEventTableDirs = determineScoreArtifactBaseEventTableAndSamplePath(
                customerSpace, modelSummary);
        String hdfsScoreArtifactBaseDir = artifactBaseAndEventTableDirs.getLeft();
        String hdfsScoreArtifactTableDir = artifactBaseAndEventTableDirs.getMiddle();
        String modelJsonType = getModelJsonType(modelSummary.getEventTableName(), hdfsScoreArtifactBaseDir);

        DataComposition dataScienceDataComposition = getModelJsonTypeHandler(modelJsonType)
                .getDataScienceDataComposition(hdfsScoreArtifactBaseDir, localPathToPersist);
        DataComposition eventTableDataComposition = getModelJsonTypeHandler(modelJsonType)
                .getEventTableDataComposition(hdfsScoreArtifactTableDir, localPathToPersist);
        Map<String, FieldSchema> mergedFields = mergeFields(eventTableDataComposition, dataScienceDataComposition);
        ModelEvaluator pmmlEvaluator = getModelEvaluator(hdfsScoreArtifactBaseDir, modelJsonType);
        File modelArtifactsDir = extractModelArtifacts(hdfsScoreArtifactBaseDir, customerSpace, modelId);
        ScoreDerivation scoreDerivation = getModelJsonTypeHandler(modelJsonType)
                .getScoreDerivation(hdfsScoreArtifactBaseDir, modelJsonType, localPathToPersist);

        ScoringArtifacts artifacts = new ScoringArtifacts(modelSummary, modelType, dataScienceDataComposition,
                eventTableDataComposition, scoreDerivation, pmmlEvaluator, modelArtifactsDir, mergedFields,
                modelJsonType, bucketMetadataList);

        return artifacts;
    }

    @VisibleForTesting
    Map<String, FieldSchema> mergeFields(DataComposition eventTableDataComposition,
            DataComposition dataScienceDataComposition) {
        Map<String, FieldSchema> mergedFields = new HashMap<>();

        Map<String, FieldSchema> eventTableFields = eventTableDataComposition.fields;
        Map<String, FieldSchema> dataScienceFields = dataScienceDataComposition.fields;

        for (String etField : eventTableFields.keySet()) {
            FieldSchema etFieldSchema = eventTableFields.get(etField);
            if (etFieldSchema.source.equals(FieldSource.REQUEST)) {
                mergedFields.put(etField, etFieldSchema);
            }
        }
        for (String dsField : dataScienceFields.keySet()) {
            FieldSchema dsFieldSchema = dataScienceFields.get(dsField);
            if (!dsFieldSchema.source.equals(FieldSource.REQUEST)) {
                mergedFields.put(dsField, dsFieldSchema);
            }
        }

        return mergedFields;
    }

    private AbstractMap.SimpleEntry<String, String> parseModelNameAndVersion(ModelSummary modelSummary) {
        String[] tokens = modelSummary.getLookupId().split("\\|");
        String modelName = tokens[1];
        String modelVersion = tokens[2];

        return new AbstractMap.SimpleEntry<String, String>(modelName, modelVersion);
    }

    /*
     * Sometimes the appId attribute in ModelSummary is empty. This method works
     * around that issue.
     */
    private String getModelAppIdSubfolder(CustomerSpace customerSpace, ModelSummary modelSummary) {
        String appId = modelSummary.getApplicationId();
        if (!StringUtils.isBlank(appId) && appId.length() > "application_".length()) {
            appId = appId.substring("application_".length());
            if (!StringUtils.isBlank(appId)) {
                log.info("Parsed appId foldername from modelsummary:" + appId);
                return appId;
            }
        }

        AbstractMap.SimpleEntry<String, String> modelNameAndVersion = parseModelNameAndVersion(modelSummary);
        String hdfsScoreArtifactAppIdDir = String.format(HDFS_SCORE_ARTIFACT_APPID_DIR, customerSpace.toString(),
                modelNameAndVersion.getKey(), modelNameAndVersion.getValue());
        try {
            List<String> folders = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsScoreArtifactAppIdDir);
            if (folders.size() == 1) {
                appId = folders.get(0).substring(folders.get(0).lastIndexOf("/") + 1);
            } else {
                throw new LedpException(LedpCode.LEDP_31007,
                        new String[] { modelSummary.getId(), JsonUtils.serialize(folders) });
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new LedpException(LedpCode.LEDP_31000, new String[] { hdfsScoreArtifactAppIdDir });
        }
        log.info("Found appId foldername by discovery:" + appId);

        return appId;
    }

    private String getScoredTxt(String hdfsScoreArtifactBaseDir) {
        String content = null;

        List<String> scoredTxtHdfsPath = null;
        try {
            scoredTxtHdfsPath = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsScoreArtifactBaseDir,
                    new HdfsFilenameFilter() {
                        @Override
                        public boolean accept(String filename) {
                            if (filename.endsWith(SCORED_TXT)) {
                                return true;
                            } else {
                                return false;
                            }
                        }
                    });
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new LedpException(LedpCode.LEDP_31018, new String[] { hdfsScoreArtifactBaseDir });
        }

        if (scoredTxtHdfsPath.size() == 1) {
            try {
                content = HdfsUtils.getHdfsFileContents(yarnConfiguration, scoredTxtHdfsPath.get(0));
            } catch (IOException e) {
                log.error(e.getMessage(), e);
                throw new LedpException(LedpCode.LEDP_31000, new String[] { scoredTxtHdfsPath.get(0) });
            }
        } else if (scoredTxtHdfsPath.size() == 0) {
            throw new LedpException(LedpCode.LEDP_31019, new String[] { hdfsScoreArtifactBaseDir });
        } else {
            throw new LedpException(LedpCode.LEDP_31020, new String[] { hdfsScoreArtifactBaseDir });
        }

        return content;
    }

    private String getModelRecordExportCsv(String hdfsScoreArtifactBaseDir) {
        String content = "";

        List<String> dataExportHdfsPath = null;
        try {
            dataExportHdfsPath = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsScoreArtifactBaseDir,
                    new HdfsFilenameFilter() {
                        @Override
                        public boolean accept(String filename) {
                            if (filename.endsWith(DATA_EXPORT_CSV)) {
                                return true;
                            } else {
                                return false;
                            }
                        }
                    });
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

        if (dataExportHdfsPath.size() == 1) {
            try {
                content = HdfsUtils.getHdfsFileContents(yarnConfiguration, dataExportHdfsPath.get(0));
            } catch (IOException e) {
                log.error(e.getMessage(), e);
                throw new LedpException(LedpCode.LEDP_31000, new String[] { dataExportHdfsPath.get(0) });
            }
        }

        return content;
    }

    private ModelJsonTypeHandler getModelJsonTypeHandler(String modelJsonType) {
        for (ModelJsonTypeHandler handler : modelJsonTypeHandlers) {
            if (handler.accept(modelJsonType)) {
                return handler;
            }
        }
        throw new LedpException(LedpCode.LEDP_31107, new String[] { modelJsonType });
    }

    private ModelEvaluator getModelEvaluator(String hdfsScoreArtifactBaseDir, String modelJsonType) {
        return getModelJsonTypeHandler(modelJsonType).getModelEvaluator(hdfsScoreArtifactBaseDir, modelJsonType,
                localPathToPersist);
    }

    private String getModelJsonType(String eventTableName, //
            String hdfsScoreArtifactBaseDir) {
        String globPath = hdfsScoreArtifactBaseDir + "*" + MODEL_JSON_SUFFIX;
        FSDataInputStream is = null;

        String modelJsonType = null;
        try (FileSystem fs = FileSystem.newInstance(yarnConfiguration)) {
            List<String> files = HdfsUtils.getFilesByGlob(yarnConfiguration, globPath);

            if (CollectionUtils.isEmpty(files)) {
                throw new LedpException(LedpCode.LEDP_31000, new String[] { globPath });
            }
            String modelJsonPath = files.get(0);

            is = fs.open(new Path(modelJsonPath));
            ObjectMapper om = new ObjectMapper();
            JsonNode node = om.readValue((InputStream) is, JsonNode.class);
            node = node.get(MODEL_KEY);
            node = node.get(MODEL_TYPE_KEY);
            String type = node.textValue();
            modelJsonType = type.substring(0, type.indexOf(DELIM));
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new LedpException(LedpCode.LEDP_31000, e, new String[] { globPath });
        }
        return modelJsonType;
    }

    private File extractModelArtifacts(String hdfsScoreArtifactBaseDir, //
            CustomerSpace customerSpace, //
            String modelId) {
        List<String> modelJsonHdfsPath = null;
        try {
            modelJsonHdfsPath = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsScoreArtifactBaseDir,
                    new HdfsFilenameFilter() {
                        @Override
                        public boolean accept(String filename) {
                            if (filename.endsWith(MODEL_JSON)) {
                                return true;
                            } else {
                                return false;
                            }
                        }
                    });
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new LedpException(LedpCode.LEDP_31001, new String[] { hdfsScoreArtifactBaseDir });
        }

        String localModelJsonCacheDir = String.format(localModelJsonCacheDirProperty, localModelJsonCacheDirIdentifier,
                customerSpace.toString(), modelId);
        File modelArtifactsDir = new File(localModelJsonCacheDir + LOCAL_MODEL_ARTIFACT_CACHE_DIR);

        // the model artifacts already exists in the local file system
        if (modelArtifactsDir.exists()) {
            return modelArtifactsDir;
        }

        if (!modelArtifactsDir.exists() && !modelArtifactsDir.mkdirs()) {
            throw new LedpException(LedpCode.LEDP_31006, new String[] { modelArtifactsDir.getAbsolutePath() });
        }

        if (modelJsonHdfsPath.size() == 1) {
            try {
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, modelJsonHdfsPath.get(0), localModelJsonCacheDir);
                if (!StringUtils.isBlank(localPathToPersist)) {
                    HdfsUtils.copyHdfsToLocal(yarnConfiguration, modelJsonHdfsPath.get(0),
                            localPathToPersist + MODEL_JSON);
                }
            } catch (IOException e) {
                log.error(e.getMessage(), e);
                throw new LedpException(LedpCode.LEDP_31002,
                        new String[] { modelJsonHdfsPath.get(0), localModelJsonCacheDir });
            }
        } else if (modelJsonHdfsPath.size() == 0) {
            throw new LedpException(LedpCode.LEDP_31003, new String[] { hdfsScoreArtifactBaseDir });
        } else {
            throw new LedpException(LedpCode.LEDP_31004, new String[] { hdfsScoreArtifactBaseDir });
        }

        List<String> modelJsonHdfsPathSplits = Splitter.on("/").splitToList(modelJsonHdfsPath.get(0));
        String modelJsonFileName = modelJsonHdfsPathSplits.get(modelJsonHdfsPathSplits.size() - 1);

        extractFromModelJson(localModelJsonCacheDir + modelJsonFileName, modelArtifactsDir.getAbsolutePath());

        return modelArtifactsDir;
    }

    @VisibleForTesting
    void extractFromModelJson(String filePath, String targetDir) {
        new ModelExtractor().extractModelArtifacts(filePath, targetDir, (dir, name) -> !name.equals(STPIPELINE_BINARY));
    }

    @Override
    public ScoringArtifacts getModelArtifacts(CustomerSpace customerSpace, //
            String modelId) {
        return scoreArtifactCache
                .getUnchecked(new AbstractMap.SimpleEntry<CustomerSpace, String>(customerSpace, modelId));
    }

    @Override
    public ScoringArtifacts getModelArtifactsIfPresent(CustomerSpace customerSpace, String modelId) {
        return scoreArtifactCache
                .getIfPresent(new AbstractMap.SimpleEntry<CustomerSpace, String>(customerSpace, modelId));
    }

    @Override
    public void updateModelArtifacts(CustomerSpace customerSpace, //
            String modelId, ScoringArtifacts scoringArtifacts) {
        scoreArtifactCache.put(new AbstractMap.SimpleEntry<CustomerSpace, String>(customerSpace, modelId),
                scoringArtifacts);
    }

    void instantiateCache() {
        log.info("Instantiating score artifact cache with max size " + scoreArtifactCacheMaxSize);
        scoreArtifactCache = CacheBuilder.newBuilder().maximumSize(scoreArtifactCacheMaxSize) //
                .expireAfterAccess(scoreArtifactCacheExpirationTime, TimeUnit.DAYS) //
                .build(new CacheLoader<AbstractMap.SimpleEntry<CustomerSpace, String>, ScoringArtifacts>() {
                    @Override
                    public ScoringArtifacts load(AbstractMap.SimpleEntry<CustomerSpace, String> key) throws Exception {
                        if (log.isInfoEnabled()) {
                            log.info(String.format("Load model artifacts for tenant %s and model %s", key.getKey(),
                                    key.getValue()));
                        }
                        ScoringArtifacts artifact = retrieveModelArtifactsFromHdfs(key.getKey(), key.getValue());
                        if (log.isInfoEnabled()) {
                            log.info(String.format("Load completed model artifacts for tenant %s and model %s",
                                    key.getKey(), key.getValue()));
                        }
                        return artifact;
                    };
                });

    }

    void scheduleRefreshJob() {
        taskScheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                refreshCache();
            }
        }, TimeUnit.SECONDS.toMillis(scoreArtifactCacheRefreshTime));
    }

    private void refreshCache() {
        log.info("Begin to refresh cache");
        List<ModelSummary> modelSummaryListNeedsToRefresh = getModelSummariesModifiedWithinTimeFrame(
                TimeUnit.SECONDS.toMillis(scoreArtifactCacheRefreshTime));
        if (CollectionUtils.isNotEmpty(modelSummaryListNeedsToRefresh)) {
            // get the modelsummary and its associated bucket metadata
            modelSummaryListNeedsToRefresh.forEach(modelsummay -> {
                CustomerSpace cs = CustomerSpace.parse(modelsummay.getTenant().getId());
                String modelId = modelsummay.getId();
                List<BucketMetadata> bucketMetadataList = getBucketMetadata(cs, modelId);
                ScoringArtifacts scoringArtifacts = getModelArtifactsIfPresent(cs, modelId);
                // lazy refresh by only updating the cache entry if present
                if (scoringArtifacts != null) {
                    scoringArtifacts.setBucketMetadataList(bucketMetadataList);
                    scoringArtifacts.setModelSummary(modelsummay);
                    updateModelArtifacts(cs, modelId, scoringArtifacts);
                    log.info(
                            String.format("Refresh cache for model %s in tenant %s finishes.", modelId, cs.toString()));
                }
            });
        }
        log.info("Refresh cache ends");
    }

    @Override
    public void setLocalPathToPersist(String localPathToPersist) {
        this.localPathToPersist = localPathToPersist;
    }

    private String determineIdFieldName(Map<String, //
            FieldSchema> fieldSchemas) {
        // find ID field
        String idFieldName = null;
        for (String fieldName : fieldSchemas.keySet()) {
            FieldSchema fieldSchema = fieldSchemas.get(fieldName);
            if (fieldSchema.interpretation == FieldInterpretation.Id) {
                idFieldName = fieldName;
                break;
            }
        }
        if (idFieldName == null) {
            throw new LedpException(LedpCode.LEDP_31021, new String[] { JsonUtils.serialize(fieldSchemas) });
        }
        return idFieldName;
    }

    @Override
    public ScoreCorrectnessArtifacts retrieveScoreCorrectnessArtifactsFromHdfs(CustomerSpace customerSpace, //
            String modelId) {
        ModelSummary modelSummary = getModelSummary(customerSpace, modelId);
        Triple<String, String, String> artifactBaseAndEventTableDirs = determineScoreArtifactBaseEventTableAndSamplePath(
                customerSpace, modelSummary);
        String hdfsScoreArtifactBaseDir = artifactBaseAndEventTableDirs.getLeft();
        String hdfsScoreArtifactTableDir = artifactBaseAndEventTableDirs.getMiddle();
        String modelJsonType = getModelJsonType(modelSummary.getEventTableName(), hdfsScoreArtifactBaseDir);

        DataComposition dataScienceDataComposition = getModelJsonTypeHandler(modelJsonType)
                .getDataScienceDataComposition(hdfsScoreArtifactBaseDir, localPathToPersist);
        DataComposition eventTableDataComposition = getModelJsonTypeHandler(modelJsonType)
                .getEventTableDataComposition(hdfsScoreArtifactTableDir, localPathToPersist);
        Map<String, FieldSchema> mergedFields = mergeFields(eventTableDataComposition, dataScienceDataComposition);
        String expectedRecords = getModelRecordExportCsv(hdfsScoreArtifactBaseDir);
        String scoredTxt = getScoredTxt(hdfsScoreArtifactBaseDir);

        ScoreCorrectnessArtifacts artifacts = new ScoreCorrectnessArtifacts();
        artifacts.setExpectedRecords(expectedRecords);
        artifacts.setScoredTxt(scoredTxt);
        artifacts.setIdField(determineIdFieldName(mergedFields));
        artifacts.setPathToSamplesAvro(artifactBaseAndEventTableDirs.getRight());
        artifacts.setFieldSchemas(mergedFields);
        return artifacts;
    }

    @Override
    public int getModelsCount(CustomerSpace customerSpace, //
            String start, //
            boolean considerAllStatus) {
        return internalResourceRestApiProxy.getModelsCount(customerSpace, start, considerAllStatus);
    }

    @Override
    public List<ModelDetail> getPaginatedModels(CustomerSpace customerSpace, //
            String start, //
            int offset, //
            int maximum, //
            boolean considerAllStatus) {
        List<ModelSummary> modelSummaries = internalResourceRestApiProxy.getPaginatedModels(customerSpace, start,
                offset, maximum, considerAllStatus);
        List<ModelDetail> models = new ArrayList<>();
        convertModelSummaryToModelDetail(models, modelSummaries, customerSpace);
        return models;
    }

    private void convertModelSummaryToModelDetail(List<ModelDetail> models, //
            List<ModelSummary> modelSummaries, //
            CustomerSpace customerSpace) {
        if (modelSummaries != null) {
            for (ModelSummary modelSummary : modelSummaries) {
                ModelDetail modelDetail = null;
                try {
                    ModelType modelType = getModelType(modelSummary.getSourceSchemaInterpretation());

                    ModelSummaryStatus status = modelSummary.getStatus();

                    Model model = new Model(modelSummary.getId(), modelSummary.getDisplayName(), modelType);
                    Long lastModifiedTimestamp = modelSummary.getLastUpdateTime();

                    Fields fields = null;
                    if (ModelSummaryStatus.DELETED.equals(status)) {
                        // if the model is deleted then there is no point in
                        // making
                        // costly operation of loading filed details we return
                        // deleted entries only to inform caller that model has
                        // been
                        // deleted. Deleted models are not used for any other
                        // purpose
                        fields = new Fields(model.getModelId(), new ArrayList<Field>());
                    } else {
                        fields = getModelFields(customerSpace, model.getModelId(), modelSummary.getPredictors());
                    }
                    modelDetail = new ModelDetail(model, status, fields,
                            BaseScoring.dateFormat.format(new Date(lastModifiedTimestamp)));
                } catch (Exception e) {
                    // skip the bad model
                    log.error(String.format("Error converting model summary to model detail for model summary: %s",
                            modelSummary.getId()));
                    continue;
                }
                models.add(modelDetail);
            }
        }
    }

    @VisibleForTesting
    String getLocalModelJsonCacheDirProperty() {
        return localModelJsonCacheDirProperty;
    }

    @VisibleForTesting
    String getLocalModelJsonCacheDirIdentifier() {
        return localModelJsonCacheDirIdentifier;
    }

}
