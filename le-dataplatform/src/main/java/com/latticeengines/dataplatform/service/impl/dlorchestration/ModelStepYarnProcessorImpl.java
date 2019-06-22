package com.latticeengines.dataplatform.service.impl.dlorchestration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.StringTokenUtils;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepYarnProcessor;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;
import com.latticeengines.domain.exposed.modeling.Algorithm;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;
import com.latticeengines.domain.exposed.modeling.algorithm.AlgorithmBase;
import com.latticeengines.domain.exposed.modeling.algorithm.LogisticRegressionAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;
import com.latticeengines.remote.exposed.service.DataLoaderService;

@Lazy
@Component("modelStepYarnProcessor")
public class ModelStepYarnProcessorImpl implements ModelStepYarnProcessor {

    private static final Logger log = LoggerFactory.getLogger(ModelStepYarnProcessorImpl.class);

    private static final String AVRO = "avro";
    private static final String SAMPLENAME_PREFIX = "s";

    enum DataSetType {
        DEPIVOTED, STANDARD
    }

    @Autowired
    private ModelingService modelingService;

    @Value("${dataplatform.dlorchestration.datasource.host}")
    private String dbHost;

    @Value("${dataplatform.dlorchestration.datasource.port}")
    private int dbPort;

    @Value("${dataplatform.dlorchestration.datasource.dbname}")
    private String dbName;

    @Value("${dataplatform.dlorchestration.datasource.user}")
    private String dbUser;

    @Value("${dataplatform.dlorchestration.datasource.password.encrypted}")
    private String dbPassword;

    @Value("${dataplatform.dlorchestration.datasource.type}")
    private String dbType;

    @Value("${dataplatform.container.virtualcores}")
    private int virtualCores;

    @Value("${dataplatform.container.memory}")
    private int memory;

    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;

    @Autowired
    private DataLoaderService dataLoaderService;

    private static final String FEATURES_THRESHOLD = "FeaturesThreshold";
    private static final String MODELING_SERVICE_NAME = "Modeling";

    @VisibleForTesting
    void setDBConfig(String dbHost, int dbPort, String dbName, String dbUser, String dbPassword, String dbType) {
        this.dbHost = dbHost;
        this.dbPort = dbPort;
        this.dbName = dbName;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
        this.dbType = dbType;
    }

    @Override
    @SuppressWarnings("incomplete-switch")
    public List<ApplicationId> executeYarnStep(String deploymentExternalId, ModelCommandStep currentStep,
            ModelCommand modelCommand, ModelCommandParameters commandParameters) {
        String tupleId = CustomerSpace.parse(deploymentExternalId).toString();

        List<ApplicationId> appIds = Collections.emptyList();
        switch (currentStep) {
        case LOAD_DATA:
            appIds = load(tupleId, modelCommand, commandParameters);
            break;
        case GENERATE_SAMPLES:
            appIds = generateSamples(tupleId, modelCommand, commandParameters);
            break;
        case PROFILE_DATA:
            appIds = profileData(tupleId, modelCommand, commandParameters);
            break;
        case SUBMIT_MODELS:
            appIds = submitModel(tupleId, modelCommand, commandParameters);
            break;
        }

        return appIds;
    }

    private List<ApplicationId> load(String customer, ModelCommand modelCommand,
            ModelCommandParameters commandParameters) {
        List<ApplicationId> appIds = new ArrayList<>();
        ApplicationId unpivotedAppId = modelingService
                .loadData(generateLoadConfiguration(DataSetType.STANDARD, customer, modelCommand, commandParameters));
        appIds.add(unpivotedAppId);
        // No LR for now.
        // ApplicationId pivotedAppId =
        // modelingService.loadData(generateLoadConfiguration(DataSetType.PIVOTED,
        // customer, commandParameters));
        // appIds.add(pivotedAppIds);

        return appIds;
    }

    private LoadConfiguration generateLoadConfiguration(DataSetType type, String customer, ModelCommand modelcommand,
            ModelCommandParameters commandParameters) {
        LoadConfiguration config = new LoadConfiguration();
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(dbHost).port(dbPort).db(dbName).user(dbUser).clearTextPassword(dbPassword).dbType(dbType);
        DbCreds creds = new DbCreds(builder);
        config.setCreds(creds);
        config.setCustomer(customer);
        if (type == DataSetType.STANDARD) {
            config.setTable(modelcommand.getEventTable());
        } else {
            config.setTable(commandParameters.getDepivotedEventTable());
        }
        config.setMetadataTable(commandParameters.getMetadataTable());
        config.setKeyCols(commandParameters.getKeyCols());

        return config;
    }

    @VisibleForTesting
    List<Integer> calculateSamplePercentages(int numSamples) {
        if (numSamples < 1)
            return Collections.emptyList();

        List<Integer> list = new ArrayList<>();

        int interval = 100 / numSamples;
        int sum = interval;
        while (sum <= (100 - interval)) {
            list.add(sum);
            sum += interval;
        }
        list.add(100);

        return list;
    }

    private List<ApplicationId> generateSamples(String customer, ModelCommand modelCommand,
            ModelCommandParameters commandParameters) {
        // No LR for now.
        // ApplicationId lrAppId =
        // modelingService.createSamples(generateSamplingConfiguration(AlgorithmType.LOGISTIC_REGRESSION,
        // customer, commandParameters));
        ApplicationId unpivotedAppId = modelingService.createSamples(
                generateSamplingConfiguration(DataSetType.STANDARD, customer, modelCommand, commandParameters));

        return Arrays.asList(/* lrAppId, */unpivotedAppId);
    }

    private String constructSampleName(int percentage) {
        return SAMPLENAME_PREFIX + String.valueOf(percentage);
    }

    @VisibleForTesting
    SamplingConfiguration generateSamplingConfiguration(DataSetType type, String customer, ModelCommand modelCommand,
            ModelCommandParameters commandParameters) {
        SamplingConfiguration samplingConfig = new SamplingConfiguration();
        samplingConfig.setTrainingPercentage(80);

        for (int percentage : calculateSamplePercentages(commandParameters.getNumSamples())) {
            SamplingElement s = new SamplingElement();
            s.setName(constructSampleName(percentage));
            s.setPercentage(percentage);
            samplingConfig.addSamplingElement(s);
        }

        samplingConfig.setCustomer(customer);

        if (type.equals(DataSetType.STANDARD)) {
            samplingConfig.setTable(modelCommand.getEventTable());
        } else {
            samplingConfig.setTable(commandParameters.getDepivotedEventTable());
        }

        return samplingConfig;
    }

    /*
     * No commented out code exists in this method to handle logistic
     * regression.
     */
    private List<ApplicationId> profileData(String customer, ModelCommand modelCommand,
            ModelCommandParameters commandParameters) {
        DataProfileConfiguration config = new DataProfileConfiguration();
        config.setCustomer(customer);
        config.setTable(modelCommand.getEventTable());
        config.setMetadataTable(commandParameters.getMetadataTable());
        config.setExcludeColumnList(commandParameters.getExcludeColumns());
        if (commandParameters.getNumSamples() > 1) {
            config.setSamplePrefix(constructSampleName(getSamplePercentage(commandParameters.getNumSamples())));
        } else {
            config.setSamplePrefix(constructSampleName(100));
        }
        config.setTargets(commandParameters.getModelTargets());
        ApplicationId appId = modelingService.profileData(config);

        return Arrays.asList(appId);
    }

    private List<ApplicationId> submitModel(String customer, ModelCommand modelCommand,
            ModelCommandParameters commandParameters) {
        List<ApplicationId> appIds = new ArrayList<>();
        List<ApplicationId> unpivotedModelAppIds = modelingService
                .submitModel(generateModel(DataSetType.STANDARD, customer, modelCommand, commandParameters));
        appIds.addAll(unpivotedModelAppIds);

        return appIds;
    }

    @VisibleForTesting
    int calculatePriority(int sampleIndex) {
        int priority = 0;
        switch (sampleIndex) {
        case 0:
            priority = 0;
            break;
        case 1:
            priority = 1;
            break;
        default:
            priority = 2;
            break;
        }

        return priority;
    }

    @VisibleForTesting
    Model generateModel(DataSetType type, String customer, ModelCommand modelCommand,
            ModelCommandParameters commandParameters) {
        List<Algorithm> algorithms = new ArrayList<>();

        int sampleIndex = 0;
        for (int percentage : calculateSamplePercentages(commandParameters.getNumSamples())) {
            AlgorithmBase algorithm;

            if (StringUtils.isNotBlank(commandParameters.getAlgorithmScript())) {
                algorithm = new AlgorithmBase();
                algorithm.setName("CUSTOM");
                algorithm.setScript(commandParameters.getAlgorithmScript());
            } else if (type.equals(DataSetType.STANDARD)) {
                algorithm = new RandomForestAlgorithm();
            } else {
                algorithm = new LogisticRegressionAlgorithm();
            }

            algorithm.resetAlgorithmProperties();
            int priority = calculatePriority(sampleIndex);
            algorithm.setPriority(calculatePriority(sampleIndex));
            algorithm.setContainerProperties(
                    "VIRTUALCORES=" + virtualCores + " MEMORY=" + memory + " PRIORITY=" + priority);
            if (StringUtils.isNotBlank(commandParameters.getAlgorithmProperties())) {
                algorithm.setAlgorithmProperties(commandParameters.getAlgorithmProperties());
            }
            algorithm.setSampleName(constructSampleName(percentage));
            algorithms.add(algorithm);

            sampleIndex++;
        }

        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName(commandParameters.getModelName());
        modelDef.addAlgorithms(algorithms);

        Model model = new Model();
        model.setModelDefinition(modelDef);
        model.setName(commandParameters.getModelName());
        if (type.equals(DataSetType.STANDARD)) {
            model.setTable(modelCommand.getEventTable());
        } else {
            model.setTable(commandParameters.getDepivotedEventTable());
        }
        model.setMetadataTable(commandParameters.getMetadataTable());
        addTemplateVersion(commandParameters);
        model.setTargetsList(commandParameters.getModelTargets());
        model.setKeyCols(commandParameters.getKeyCols());
        model.setCustomer(customer);
        model.setDataFormat(AVRO);
        model.setProvenanceProperties(generateProvenanceProperties(commandParameters));
        setModelConfigurationFromCamille(model, customer);
        model.setFeaturesThreshold(generateFeatureThreshold(commandParameters));

        List<String> features = modelingService.getFeatures(model, false);
        model.setFeaturesList(features);

        return model;
    }

    private void setModelConfigurationFromCamille(Model model, String deploymentExternalId) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode featuresThresholdNode = null;
        try {
            Document document = getFeatureThresholdConfigDocumentDirectory(
                    CustomerSpace.parse(deploymentExternalId).toString());
            String featuresThresholdDocument = document.getData();
            featuresThresholdNode = mapper.readTree(featuresThresholdDocument);
            int featureThreshold = featuresThresholdNode.asInt();
            if (featureThreshold < 1) {
                // -1 is a special value for features threshold. -1 instructs
                // the model to use all features
                // This if block checks if the value has been set to 0 or <1,
                // and in that case, sets it to the special value of -1.
                model.setFeaturesThreshold(-1);
            } else if (featureThreshold < 50) {
                model.setFeaturesThreshold(-1);
            } else {
                model.setFeaturesThreshold(featureThreshold);
            }
        } catch (JsonProcessingException e) {
            log.error("Could not parse Features Threshold from JSON:" + featuresThresholdNode + e.toString());
        } catch (IOException e) {
            log.error("IO Error getting features threshold:" + featuresThresholdNode + e.toString());
        } catch (Exception e) {
            log.error("Error getting features threshold:" + featuresThresholdNode + e.toString());
        }
    }

    public Document getFeatureThresholdConfigDocumentDirectory(String customerSpace) {
        try {
            Camille camille = CamilleEnvironment.getCamille();
            Path docPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(),
                    CustomerSpace.parse(customerSpace), MODELING_SERVICE_NAME);
            docPath = docPath.append(FEATURES_THRESHOLD);
            return camille.get(docPath);
        } catch (Exception e) {
            log.error("Error getting features threshold configs from Customer Space:" + customerSpace);
            return null;
        }
    }

    private void addTemplateVersion(ModelCommandParameters commandParameters) {
        if (commandParameters == null || commandParameters.getModelTargets() == null
                || commandParameters.getModelTargets().size() <= 1) {
            return;
        }
        if (dataLoaderService == null) {
            return;
        }
        List<String> targets = commandParameters.getModelTargets();
        String templateVersion = dataLoaderService.getTemplateVersion(commandParameters.getDlTenant(),
                commandParameters.getDlUrl());
        List<String> newTargets = new ArrayList<>(targets);
        if (!StringUtils.isEmpty(templateVersion)) {
            templateVersion = templateVersion.replaceAll("[:]+", "_");
        }
        newTargets.add("Template_Version:" + templateVersion);
        commandParameters.setModelTargets(newTargets);
    }

    private String generateProvenanceProperties(ModelCommandParameters commandParameters) {
        Properties provenanceProperties = new Properties();
        provenanceProperties.put(ModelCommandParameters.DL_URL, commandParameters.getDlUrl());
        provenanceProperties.put(ModelCommandParameters.DL_TENANT, commandParameters.getDlTenant());
        provenanceProperties.put(ModelCommandParameters.DL_QUERY, commandParameters.getDlQuery());

        return StringTokenUtils.propertyToString(provenanceProperties);
    }

    @VisibleForTesting
    int generateFeatureThreshold(ModelCommandParameters commandParameters) {
        int featureThreshold = -1;
        if (commandParameters == null || commandParameters.getAlgorithmProperties() == null
                || commandParameters.getAlgorithmProperties().trim().length() == 0) {
            return featureThreshold;
        }

        String[] properties = commandParameters.getAlgorithmProperties().split("\\s+");
        for (String prop : properties) {
            if (prop.toLowerCase().startsWith(ModelCommandParameters.FEATURE_THRESHOLD.toLowerCase())) {
                String[] tokens = prop.split("=");
                if (tokens.length == 2) {
                    featureThreshold = Integer.parseInt(tokens[1]);
                    break;
                }
            }
        }
        return featureThreshold;
    }

    @VisibleForTesting
    int getSamplePercentage(int numberOfSamples) {
        int samplePercentage = 100;
        if (numberOfSamples <= 1) {
            return samplePercentage;
        } else {
            List<Integer> samplesList = calculateSamplePercentages(numberOfSamples);
            samplePercentage = samplesList.get(0);
            return samplePercentage;
        }
    }
}
