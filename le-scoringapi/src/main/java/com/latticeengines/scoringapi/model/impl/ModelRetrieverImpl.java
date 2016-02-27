package com.latticeengines.scoringapi.model.impl;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.domain.exposed.scoringapi.ModelIdentifier;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.scoringapi.controller.InternalResourceRestApiProxy;
import com.latticeengines.scoringapi.exposed.Field;
import com.latticeengines.scoringapi.exposed.Fields;
import com.latticeengines.scoringapi.exposed.Model;
import com.latticeengines.scoringapi.exposed.ModelArtifacts;
import com.latticeengines.scoringapi.exposed.ModelType;
import com.latticeengines.scoringapi.infrastructure.ScoringProperties;
import com.latticeengines.scoringapi.model.ModelEvaluator;
import com.latticeengines.scoringapi.model.ModelRetriever;

@Component("modelRetriever")
public class ModelRetrieverImpl implements ModelRetriever {

    private static final Log log = LogFactory.getLog(ModelRetrieverImpl.class);
    private static final String HDFS_MODEL_ARTIFACT_BASE_DIR = "/user/s-analytics/customers/%s/models/%s/%s/%s/";
    private static final String HDFS_ENHANCEMENTS_DIR = "enhancements/";
    private static final String PMML_FILENAME = "rfpmml.xml";
    private static final String SCORE_DERIVATION_FILENAME = "scorederivation.json";
    private static final String DATA_COMPOSITION_FILENAME = "datacomposition.json";

    @Value("${scoringapi.pls.api.hostport}")
    private String internalResourceHostPort;

    @Value("${scoringapi.modelartifact.cache.maxsize}")
    private int modelArtifactCacheMaxSize;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ScoringProperties properties;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    private LoadingCache<AbstractMap.SimpleEntry<CustomerSpace, ModelIdentifier>, ModelEvaluator> cache;

    private LoadingCache<AbstractMap.SimpleEntry<CustomerSpace, String>, ModelArtifacts> modelArtifactCache;

    @PostConstruct
    public void initialize() throws Exception {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
        instantiateCache();
    }

    public ModelEvaluator getEvaluator(CustomerSpace space, ModelIdentifier model) {
        return cache.getUnchecked(new AbstractMap.SimpleEntry<CustomerSpace, ModelIdentifier>(space, model));
    }

    @Override
    public List<Model> getActiveModels(CustomerSpace customerSpace, ModelType type) {
        List<Model> models = new ArrayList<>();

        List<?> modelSummaries = internalResourceRestApiProxy.getActiveModelSummaries(customerSpace);
        if (modelSummaries != null) {
            for (Object modelSummary : modelSummaries) {
                @SuppressWarnings("unchecked")
                Map<String, String> map = (Map<String, String>) modelSummary;
                Model model = new Model(map.get("Id"), map.get("Name"), type);
                models.add(model);
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

        ModelArtifacts artifacts = getModelArtifacts(customerSpace, modelId);
        Map<String, FieldSchema> mapFields = artifacts.getDataComposition().fields;
        for (String fieldName : mapFields.keySet()) {
            FieldSchema fieldSchema = mapFields.get(fieldName);
            if (fieldSchema.source.equals(FieldSource.REQUEST)) {
                Field field = new Field(fieldName, fieldSchema.type);
                fieldList.add(field);
            }
        }

        return fields;
    }

    private ModelArtifacts retrieveModelArtifacts(CustomerSpace customerSpace, String modelId) {
        ModelSummary modelSummary = internalResourceRestApiProxy.getModelSummaryFromModelId(modelId, customerSpace);

        String[] tokens = modelSummary.getLookupId().split("\\|");
        String modelName = tokens[1];
        String modelVersion = tokens[2];
        String appId = modelSummary.getApplicationId().substring("application_".length());

        String hdfsModelArtifactBaseDir = String.format(HDFS_MODEL_ARTIFACT_BASE_DIR, customerSpace.toString(),
                modelName, modelVersion, appId);

        DataComposition dataComposition = getDataComposition(hdfsModelArtifactBaseDir);
        ScoreDerivation scoreDerivation = getScoreDerivation(hdfsModelArtifactBaseDir);
        ModelEvaluator pmmlEvaluator = getModelEvaluator(hdfsModelArtifactBaseDir);

        ModelArtifacts artifacts = new ModelArtifacts(modelSummary.getId(), dataComposition, scoreDerivation,
                pmmlEvaluator);

        return artifacts;
    }

    private DataComposition getDataComposition(String hdfsModelArtifactBaseDir) {
        String path = hdfsModelArtifactBaseDir + HDFS_ENHANCEMENTS_DIR + DATA_COMPOSITION_FILENAME;
        String content = null;
        try {
            content = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_31000, new String[] { path });
        }
        DataComposition dataComposition = JsonUtils.deserialize(content, DataComposition.class);
        return dataComposition;
    }

    private ScoreDerivation getScoreDerivation(String hdfsModelArtifactBaseDir) {
        String path = hdfsModelArtifactBaseDir + HDFS_ENHANCEMENTS_DIR + SCORE_DERIVATION_FILENAME;
        String content = null;
        try {
            content = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_31000, new String[] { path });
        }
        ScoreDerivation scoreDerivation = JsonUtils.deserialize(content, ScoreDerivation.class);
        return scoreDerivation;
    }

    private ModelEvaluator getModelEvaluator(String hdfsModelArtifactBaseDir) {
        String path = hdfsModelArtifactBaseDir + PMML_FILENAME;
        FSDataInputStream is = null;

        ModelEvaluator modelEvaluator = null;
        try {
            FileSystem fs = FileSystem.newInstance(yarnConfiguration);
            is = fs.open(new Path(path));
            modelEvaluator = new ModelEvaluator(is);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_31000, new String[] { path });
        }
        return modelEvaluator;
    }

    @Override
    public ModelArtifacts getModelArtifacts(CustomerSpace customerSpace, String modelId) {
        return modelArtifactCache.getUnchecked(new AbstractMap.SimpleEntry<CustomerSpace, String>(customerSpace, modelId));
    }

    private void instantiateCache() {
        modelArtifactCache = CacheBuilder.newBuilder().maximumSize(modelArtifactCacheMaxSize)
                .build(new CacheLoader<AbstractMap.SimpleEntry<CustomerSpace, String>, ModelArtifacts>() {
                    @Override
                    public ModelArtifacts load(AbstractMap.SimpleEntry<CustomerSpace, String> key)
                            throws Exception {
                        return retrieveModelArtifacts(key.getKey(), key.getValue());
                    }
                });
    }
}
