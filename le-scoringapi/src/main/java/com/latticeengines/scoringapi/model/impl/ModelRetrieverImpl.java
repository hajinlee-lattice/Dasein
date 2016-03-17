package com.latticeengines.scoringapi.model.impl;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.python.google.common.base.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.common.exposed.modeling.ModelExtractor;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.scoringapi.controller.InternalResourceRestApiProxy;
import com.latticeengines.scoringapi.exception.ScoringApiException;
import com.latticeengines.scoringapi.exposed.Field;
import com.latticeengines.scoringapi.exposed.Fields;
import com.latticeengines.scoringapi.exposed.Model;
import com.latticeengines.scoringapi.exposed.ModelType;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;
import com.latticeengines.scoringapi.model.ModelEvaluator;
import com.latticeengines.scoringapi.model.ModelRetriever;
import com.latticeengines.scoringapi.warnings.Warnings;

@Component("modelRetriever")
public class ModelRetrieverImpl implements ModelRetriever {

    private static final Log log = LogFactory.getLog(ModelRetrieverImpl.class);
    public static final String HDFS_SCORE_ARTIFACT_TABLE_DIR = "/user/s-analytics/customers/%s/data/%s-Event-Metadata/";
    public static final String HDFS_SCORE_ARTIFACT_APPID_DIR = "/user/s-analytics/customers/%s/models/%s/%s/";
    public static final String HDFS_SCORE_ARTIFACT_BASE_DIR = HDFS_SCORE_ARTIFACT_APPID_DIR + "%s/";
    public static final String HDFS_ENHANCEMENTS_DIR = "enhancements/";
    public static final String PMML_FILENAME = "rfpmml.xml";
    public static final String SCORE_DERIVATION_FILENAME = "scorederivation.json";
    public static final String DATA_COMPOSITION_FILENAME = "datacomposition.json";
    public static final String MODEL_JSON = "model.json";
    private static final String LOCAL_MODELJSON_CACHE_DIR = "/var/cache/scoringapi/%s/%s/"; // space
                                                                                            // modelId
    private static final String LOCAL_MODEL_ARTIFACT_CACHE_DIR = "artifacts/";

    @Value("${scoringapi.pls.api.hostport}")
    private String internalResourceHostPort;

    @Value("${scoringapi.scoreartifact.cache.maxsize}")
    private int scoreArtifactCacheMaxSize;

    @Autowired
    private Warnings warnings;

    @Autowired
    private Configuration yarnConfiguration;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    private LoadingCache<AbstractMap.SimpleEntry<CustomerSpace, String>, ScoringArtifacts> scoreArtifactCache;

    private String localPathToPersist = null;

    @PostConstruct
    public void initialize() throws Exception {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
        instantiateCache();
    }

    @Override
    public List<Model> getActiveModels(CustomerSpace customerSpace, ModelType type) {
        List<Model> models = new ArrayList<>();

        List<?> modelSummaries = internalResourceRestApiProxy.getActiveModelSummaries(customerSpace);
        if (modelSummaries != null) {
            for (Object modelSummary : modelSummaries) {
                @SuppressWarnings("unchecked")
                Map<String, String> map = (Map<String, String>) modelSummary;
                if ((type == ModelType.ACCOUNT && map.get("SourceSchemaInterpretation").toLowerCase()
                        .contains("account"))
                        || (type == ModelType.CONTACT && map.get("SourceSchemaInterpretation").toLowerCase()
                                .contains("lead"))) {
                    Model model = new Model(map.get("Id"), map.get("Name"), type);
                    models.add(model);
                }
            }
        }

        return models;
    }

    @Override
    public Fields getModelFields(CustomerSpace customerSpace, String modelId) {
        Fields fields = new Fields();
        fields.setModelId(modelId);
        List<Field> fieldList = new ArrayList<>();
        fields.setFields(fieldList);

        ScoringArtifacts artifacts = getModelArtifacts(customerSpace, modelId);
        Map<String, FieldSchema> mapFields = artifacts.getDataScienceDataComposition().fields;
        for (String fieldName : mapFields.keySet()) {
            FieldSchema fieldSchema = mapFields.get(fieldName);
            if (fieldSchema.source.equals(FieldSource.REQUEST)) {
                Field field = new Field(fieldName, fieldSchema.type);
                fieldList.add(field);
            }
        }

        return fields;
    }

    @Override
    public ScoringArtifacts retrieveModelArtifactsFromHdfs(CustomerSpace customerSpace, String modelId) {
        ModelSummary modelSummary = internalResourceRestApiProxy.getModelSummaryFromModelId(modelId, customerSpace);
        if (modelSummary == null) {
            throw new ScoringApiException(LedpCode.LEDP_31102, new String[] { modelId });
        } else if (Strings.isNullOrEmpty(modelSummary.getEventTableName())) {
            throw new LedpException(LedpCode.LEDP_31008, new String[] { modelId });
        }

        AbstractMap.SimpleEntry<String, String> modelNameAndVersion = parseModelNameAndVersion(modelSummary);
        String appId = getModelAppIdSubfolder(customerSpace, modelSummary);

        String hdfsScoreArtifactBaseDir = String.format(HDFS_SCORE_ARTIFACT_BASE_DIR, customerSpace.toString(),
                modelNameAndVersion.getKey(), modelNameAndVersion.getValue(), appId);

        String hdfsScoreArtifactTableDir = String.format(HDFS_SCORE_ARTIFACT_TABLE_DIR, customerSpace.toString(),
                modelSummary.getEventTableName());

        DataComposition dataScienceDataComposition = getDataScienceDataComposition(hdfsScoreArtifactBaseDir);
        DataComposition eventTableDataComposition = getEventTableDataComposition(hdfsScoreArtifactTableDir);
        removeDroppedDataScienceFieldEventTableTransforms(eventTableDataComposition, dataScienceDataComposition);
        ScoreDerivation scoreDerivation = getScoreDerivation(hdfsScoreArtifactBaseDir);
        ModelEvaluator pmmlEvaluator = getModelEvaluator(hdfsScoreArtifactBaseDir);
        File modelArtifactsDir = extractModelArtifacts(hdfsScoreArtifactBaseDir, customerSpace, modelId);

        ScoringArtifacts artifacts = new ScoringArtifacts(modelSummary.getId(), dataScienceDataComposition,
                eventTableDataComposition, scoreDerivation, pmmlEvaluator, modelArtifactsDir);

        return artifacts;
    }

    @VisibleForTesting
    void removeDroppedDataScienceFieldEventTableTransforms(DataComposition eventTableDataComposition,
            DataComposition dataScienceDataComposition) {
        Set<String> difference = new HashSet<>(eventTableDataComposition.fields.keySet());
        if (log.isDebugEnabled()) {
            log.debug("Difference in datacompositions:" + JsonUtils.serialize(difference));
        }
        difference.removeAll(dataScienceDataComposition.fields.keySet());

        List<TransformDefinition> transformsToKeep = new ArrayList<>();

        for (TransformDefinition transformDefinition : eventTableDataComposition.transforms) {
            boolean keepTransform = true;
            for (Object argObject : transformDefinition.arguments.values()) {
                if (argObject != null) {
                    String argValue = String.valueOf(argObject);
                    if (difference.contains(argValue)) {
                        keepTransform = false;
                    }
                }
            }
            if (keepTransform) {
                transformsToKeep.add(transformDefinition);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Removing this transform:" + JsonUtils.serialize(transformDefinition));
                }
            }

        }

        eventTableDataComposition.transforms = transformsToKeep;
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
        String appId = modelSummary.getApplicationId().substring("application_".length());
        if (!Strings.isNullOrEmpty(appId)) {
            return appId;
        }

        AbstractMap.SimpleEntry<String, String> modelNameAndVersion = parseModelNameAndVersion(modelSummary);
        String hdfsScoreArtifactAppIdDir = String.format(HDFS_SCORE_ARTIFACT_APPID_DIR, customerSpace.toString(),
                modelNameAndVersion.getKey(), modelNameAndVersion.getValue());
        try {
            List<String> folders = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsScoreArtifactAppIdDir);
            if (folders.size() == 1) {
                appId = folders.get(0);
            } else {
                throw new LedpException(LedpCode.LEDP_31007, new String[] { modelSummary.getId(),
                        JsonUtils.serialize(folders) });
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_31000, new String[] { hdfsScoreArtifactAppIdDir });
        }

        return appId;
    }

    private DataComposition getDataScienceDataComposition(String hdfsScoreArtifactBaseDir) {
        String path = hdfsScoreArtifactBaseDir + HDFS_ENHANCEMENTS_DIR + DATA_COMPOSITION_FILENAME;
        String content = null;
        try {
            content = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);
            if (!Strings.isNullOrEmpty(localPathToPersist)) {
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, path, localPathToPersist + DATA_COMPOSITION_FILENAME);
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_31000, new String[] { path });
        }
        DataComposition dataComposition = JsonUtils.deserialize(content, DataComposition.class);
        return dataComposition;
    }

    private DataComposition getEventTableDataComposition(String hdfsScoreArtifactTableDir) {
        String path = hdfsScoreArtifactTableDir + DATA_COMPOSITION_FILENAME;
        String content = null;
        try {
            content = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);
            if (!Strings.isNullOrEmpty(localPathToPersist)) {
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, path, localPathToPersist + "metadata-"
                        + DATA_COMPOSITION_FILENAME);
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_31000, new String[] { path });
        }
        DataComposition dataComposition = JsonUtils.deserialize(content, DataComposition.class);
        return dataComposition;
    }

    private ScoreDerivation getScoreDerivation(String hdfsScoreArtifactBaseDir) {
        String path = hdfsScoreArtifactBaseDir + HDFS_ENHANCEMENTS_DIR + SCORE_DERIVATION_FILENAME;
        String content = null;
        try {
            content = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);
            if (!Strings.isNullOrEmpty(localPathToPersist)) {
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, path, localPathToPersist + SCORE_DERIVATION_FILENAME);
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_31000, new String[] { path });
        }
        ScoreDerivation scoreDerivation = JsonUtils.deserialize(content, ScoreDerivation.class);
        return scoreDerivation;
    }

    private ModelEvaluator getModelEvaluator(String hdfsScoreArtifactBaseDir) {
        String path = hdfsScoreArtifactBaseDir + PMML_FILENAME;
        FSDataInputStream is = null;

        ModelEvaluator modelEvaluator = null;
        try {
            FileSystem fs = FileSystem.newInstance(yarnConfiguration);
            is = fs.open(new Path(path));
            modelEvaluator = new ModelEvaluator(is);
            if (!Strings.isNullOrEmpty(localPathToPersist)) {
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, path, localPathToPersist + PMML_FILENAME);
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_31000, new String[] { path });
        }
        return modelEvaluator;
    }

    private File extractModelArtifacts(String hdfsScoreArtifactBaseDir, CustomerSpace customerSpace, String modelId) {
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
            throw new LedpException(LedpCode.LEDP_31001, new String[] { hdfsScoreArtifactBaseDir });
        }

        String localModelJsonCacheDir = String.format(LOCAL_MODELJSON_CACHE_DIR, customerSpace.toString(), modelId);
        File modelArtifactsDir = new File(localModelJsonCacheDir + LOCAL_MODEL_ARTIFACT_CACHE_DIR);

        if (!modelArtifactsDir.exists() && !modelArtifactsDir.mkdirs()) {
            throw new LedpException(LedpCode.LEDP_31006, new String[] { modelArtifactsDir.getAbsolutePath() });
        }

        if (modelJsonHdfsPath.size() == 1) {
            try {
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, modelJsonHdfsPath.get(0), localModelJsonCacheDir);
                if (!Strings.isNullOrEmpty(localPathToPersist)) {
                    HdfsUtils.copyHdfsToLocal(yarnConfiguration, modelJsonHdfsPath.get(0), localPathToPersist
                            + MODEL_JSON);
                }
            } catch (IOException e) {
                throw new LedpException(LedpCode.LEDP_31002, new String[] { modelJsonHdfsPath.get(0),
                        localModelJsonCacheDir });
            }
        } else if (modelJsonHdfsPath.size() == 0) {
            throw new LedpException(LedpCode.LEDP_31003, new String[] { hdfsScoreArtifactBaseDir });
        } else {
            throw new LedpException(LedpCode.LEDP_31004, new String[] { hdfsScoreArtifactBaseDir });
        }

        List<String> modelJsonHdfsPathSplits = Splitter.on("/").splitToList(modelJsonHdfsPath.get(0));
        String modelJsonFileName = modelJsonHdfsPathSplits.get(modelJsonHdfsPathSplits.size() - 1);

        new ModelExtractor().extractModelArtifacts(localModelJsonCacheDir + modelJsonFileName,
                modelArtifactsDir.getAbsolutePath());

        return modelArtifactsDir;
    }

    @Override
    public ScoringArtifacts getModelArtifacts(CustomerSpace customerSpace, String modelId) {
        return scoreArtifactCache.getUnchecked(new AbstractMap.SimpleEntry<CustomerSpace, String>(customerSpace,
                modelId));
    }

    private void instantiateCache() {
        log.info("Instantiating score artifact cache with max size " + scoreArtifactCacheMaxSize);
        scoreArtifactCache = CacheBuilder.newBuilder().maximumSize(scoreArtifactCacheMaxSize)
                .build(new CacheLoader<AbstractMap.SimpleEntry<CustomerSpace, String>, ScoringArtifacts>() {
                    @Override
                    public ScoringArtifacts load(AbstractMap.SimpleEntry<CustomerSpace, String> key) throws Exception {
                        log.info("Loading model artifacts");

                        return retrieveModelArtifactsFromHdfs(key.getKey(), key.getValue());
                    }
                });
    }

    @Override
    public void setLocalPathToPersist(String localPathToPersist) {
        this.localPathToPersist = localPathToPersist;
    }
}
