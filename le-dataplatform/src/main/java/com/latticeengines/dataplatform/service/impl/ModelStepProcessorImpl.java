package com.latticeengines.dataplatform.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.service.ModelCommandLogService;
import com.latticeengines.dataplatform.service.ModelStepProcessor;
import com.latticeengines.dataplatform.service.impl.dlorchestration.ModelCommandParameters;
import com.latticeengines.domain.exposed.dataplatform.Algorithm;
import com.latticeengines.domain.exposed.dataplatform.DataProfileConfiguration;
import com.latticeengines.domain.exposed.dataplatform.DbCreds;
import com.latticeengines.domain.exposed.dataplatform.LoadConfiguration;
import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.domain.exposed.dataplatform.ModelDefinition;
import com.latticeengines.domain.exposed.dataplatform.SamplingConfiguration;
import com.latticeengines.domain.exposed.dataplatform.SamplingElement;
import com.latticeengines.domain.exposed.dataplatform.algorithm.AlgorithmBase;
import com.latticeengines.domain.exposed.dataplatform.algorithm.LogisticRegressionAlgorithm;
import com.latticeengines.domain.exposed.dataplatform.algorithm.RandomForestAlgorithm;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandParameter;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;

@Component("modelStepProcessor")
public class ModelStepProcessorImpl implements ModelStepProcessor {
    
    private static final char COMMA = ',';
    private static final String AVRO = "avro";
    private static final String SAMPLENAME_PREFIX = "s";
    
    static enum AlgorithmType {
        LOGISTIC_REGRESSION, RANDOM_FOREST; 
    }
    
    @Autowired
    private ModelingService modelingService;
    
    @Autowired
    private Configuration yarnConfiguration;
    
    @Autowired
    private ModelCommandLogService modelCommandLogService;
    
    @Value("${dataplatform.dlorchestration.datasource.host}")
    private String dbHost;
    
    @Value("${dataplatform.dlorchestration.datasource.port}")
    private int dbPort;
    
    @Value("${dataplatform.dlorchestration.datasource.dbname}")
    private String dbName;
    
    @Value("${dataplatform.dlorchestration.datasource.user}")
    private String dbUser;
    
    @Value("${dataplatform.dlorchestration.datasource.password}")
    private String dbPassword;
   
    @Value("${dataplatform.dlorchestration.datasource.type}")
    private String dbType;
    
    @Value("${dataplatform.container.virtualcores}")
    private int virtualCores;
    
    @Value("${dataplatform.container.memory}")
    private int memory;
    
    @Override
    @SuppressWarnings("incomplete-switch")
    public List<ApplicationId> executeYarnStep(String deploymentExternalId, ModelCommandStep currentStep, List<ModelCommandParameter> commandParameters) {
        ModelCommandParameters parameters = validateCommandParameters(commandParameters);
        
        List<ApplicationId> appIds = Collections.emptyList();
        switch (currentStep) {
        case LOAD_DATA:                                                     
            appIds = load(deploymentExternalId, parameters);
            break;    
        case GENERATE_SAMPLES:
            appIds = generateSamples(deploymentExternalId, parameters);
            break;
        case PROFILE_DATA:
            appIds = profileData(deploymentExternalId, parameters);
            break;
        case SUBMIT_MODELS:
            appIds = submitModel(deploymentExternalId, parameters);
            break;
        }
        
        return appIds;
    }
    
    @VisibleForTesting
    ModelCommandParameters validateCommandParameters(List<ModelCommandParameter> commandParameters) {
        ModelCommandParameters modelCommandParameters = new ModelCommandParameters();
        
        for (ModelCommandParameter parameter : commandParameters) {
            switch (parameter.getKey()) { 
            case ModelCommandParameters.DEPIVOTED_EVENT_TABLE:
                modelCommandParameters.setDepivotedEventTable(parameter.getValue());
                break;
            case ModelCommandParameters.EVENT_TABLE:
                modelCommandParameters.setEventTable(parameter.getValue());
                break;
            case ModelCommandParameters.KEY_COLS:
                modelCommandParameters.setKeyCols(splitCommaSeparatedStringToList(parameter.getValue()));
                break;
            case ModelCommandParameters.MODEL_NAME:
                modelCommandParameters.setModelName(parameter.getValue());
                break;
            case ModelCommandParameters.MODEL_TARGETS:
                modelCommandParameters.setModelTargets(splitCommaSeparatedStringToList(parameter.getValue()));
                break;
            case ModelCommandParameters.NUM_SAMPLES:
                modelCommandParameters.setNumSamples(Integer.parseInt(parameter.getValue()));
                break;
            case ModelCommandParameters.EXCLUDE_COLUMNS:
                modelCommandParameters.setExcludeColumns(splitCommaSeparatedStringToList(parameter.getValue()));
                break;

            }
        }
        
        List<String> missingParameters = new ArrayList<>();
        if (Strings.isNullOrEmpty(modelCommandParameters.getEventTable())) {
            missingParameters.add(ModelCommandParameters.EVENT_TABLE);
        } 
        if (modelCommandParameters.getKeyCols().isEmpty()) {
            missingParameters.add(ModelCommandParameters.KEY_COLS);
        } 
        if (Strings.isNullOrEmpty(modelCommandParameters.getModelName())) {
            missingParameters.add(ModelCommandParameters.MODEL_NAME);
        }
        if (modelCommandParameters.getModelTargets().isEmpty()) {
            missingParameters.add(ModelCommandParameters.MODEL_TARGETS);
        }
        if (modelCommandParameters.getExcludeColumns().isEmpty()) {
            missingParameters.add(ModelCommandParameters.EXCLUDE_COLUMNS);
        }
        
        if (!missingParameters.isEmpty()) {
            throw new LedpException(LedpCode.LEDP_16000, new String[] { missingParameters.toString() } );
        }        
        
        return modelCommandParameters;
    }
    
    @VisibleForTesting
    List<String> splitCommaSeparatedStringToList(String input) {
        if (Strings.isNullOrEmpty(input)) {
            return Collections.emptyList();
        } else {
            return Splitter.on(COMMA).trimResults().omitEmptyStrings().splitToList(input);
        }
    }
    
    private List<ApplicationId> load(String customer, ModelCommandParameters commandParameters) {
        String deletePath = "/user/s-analytics/customers/" + customer + "/data";
        try (FileSystem fs = FileSystem.get(yarnConfiguration)) {                 
            if (fs.exists(new Path(deletePath))) {
                boolean result = fs.delete(new Path(deletePath), true);
                if (!result) {
                    throw new LedpException(LedpCode.LEDP_16001, new String[] { deletePath });
                }
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_16001, e, new String[] { deletePath });            
        }
                       
        List<ApplicationId> appIds = new ArrayList<>(); 
        ApplicationId pivotedAppId = modelingService.loadData(generateLoadConfiguration(AlgorithmType.RANDOM_FOREST, customer, commandParameters));
        appIds.add(pivotedAppId);
        // No LR for now.
//        ApplicationId depivotedAppId = modelingService.loadData(generateLoadConfiguration(AlgorithmType.LOGISTIC_REGRESSION, customer, commandParameters));        
//        appIds.add(depivotedAppIds);
        
        return appIds;
    }
    
    private LoadConfiguration generateLoadConfiguration(AlgorithmType type, String customer, ModelCommandParameters commandParameters) {
        LoadConfiguration config = new LoadConfiguration();
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(dbHost).port(dbPort).db(dbName).user(dbUser).password(dbPassword).type(dbType);
        DbCreds creds = new DbCreds(builder);
        config.setCreds(creds);
        config.setCustomer(customer);
        if (type == AlgorithmType.RANDOM_FOREST) {
            config.setTable(commandParameters.getEventTable());
        } else {
            config.setTable(commandParameters.getDepivotedEventTable());            
        }
        config.setMetadataTable(commandParameters.getMetadataTable());
        config.setKeyCols(commandParameters.getKeyCols());
        
        return config;
    }
    
    @VisibleForTesting
    List<Integer> calculateSamplePercentages(int numSamples) {
        if (numSamples < 1) return Collections.emptyList();
        
        List<Integer> list = new ArrayList<>();       
        
        int interval = 100/numSamples;
        int sum = interval;
        while (sum <= (100-interval)) {
            list.add(sum);
            sum += interval;
        }           
        list.add(100);
        
        return list;
    }
    
    private List<ApplicationId> generateSamples(String customer, ModelCommandParameters commandParameters) {
        // No LR for now.
//        ApplicationId lrAppId = modelingService.createSamples(generateSamplingConfiguration(AlgorithmType.LOGISTIC_REGRESSION, customer, commandParameters));
        ApplicationId rfAppId = modelingService.createSamples(generateSamplingConfiguration(AlgorithmType.RANDOM_FOREST, customer, commandParameters));

        return Arrays.asList(/*lrAppId, */rfAppId);
    }
  
    private String constructSampleName(int percentage) {
        return SAMPLENAME_PREFIX + String.valueOf(percentage);
    }
    
    @VisibleForTesting
    SamplingConfiguration generateSamplingConfiguration(AlgorithmType type, String customer, ModelCommandParameters commandParameters) {
        SamplingConfiguration samplingConfig = new SamplingConfiguration();
        samplingConfig.setTrainingPercentage(80);
        
        for (int percentage : calculateSamplePercentages(commandParameters.getNumSamples())) {
            SamplingElement s = new SamplingElement();
            s.setName(constructSampleName(percentage));
            s.setPercentage(percentage);
            samplingConfig.addSamplingElement(s);                 
        }       
        
        samplingConfig.setCustomer(customer);      
        
        if (type.equals(AlgorithmType.RANDOM_FOREST)) {
            samplingConfig.setTable(commandParameters.getEventTable());
        } else {
            samplingConfig.setTable(commandParameters.getDepivotedEventTable());            
        }
        
        return samplingConfig;
    }  
    
    /*
     * No commented out code exists in this method to handle logistic regression.
     */
    private List<ApplicationId> profileData(String customer, ModelCommandParameters commandParameters) {
        DataProfileConfiguration config = new DataProfileConfiguration();
        config.setCustomer(customer);
        config.setTable(commandParameters.getEventTable());
        config.setMetadataTable(commandParameters.getMetadataTable());
        config.setExcludeColumnList(commandParameters.getExcludeColumns());
        config.setSamplePrefix(SAMPLENAME_PREFIX+"100");
        ApplicationId appId = modelingService.profileData(config);
        
        return Arrays.asList(appId);        
    }
    
    private List<ApplicationId> submitModel(String customer, ModelCommandParameters commandParameters) {
        List<ApplicationId> appIds = new ArrayList<>();
        // No LR for now.
        List<ApplicationId> rfAppIds = modelingService.submitModel(generateModel(AlgorithmType.RANDOM_FOREST, customer, commandParameters));
//        List<ApplicationId> lrAppIds = modelingService.submitModel(generateModel(AlgorithmType.LOGISTIC_REGRESSION, customer, commandParameters));
        
        appIds.addAll(rfAppIds);
//        appIds.addAll(lrAppIds);
   
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
    Model generateModel(AlgorithmType type, String customer, ModelCommandParameters commandParameters) {
        List<Algorithm> algorithms = new ArrayList<>();
        
        int sampleIndex = 0;
        for (int percentage : calculateSamplePercentages(commandParameters.getNumSamples())) {
            AlgorithmBase algorithm;
            if (type.equals(AlgorithmType.RANDOM_FOREST)) {
                algorithm = new RandomForestAlgorithm();
            } else {
                algorithm = new LogisticRegressionAlgorithm();                
            }
                      
            int priority = calculatePriority(sampleIndex);
            algorithm.setPriority(calculatePriority(sampleIndex));
            algorithm.setContainerProperties("VIRTUALCORES=" + virtualCores + " MEMORY=" + memory + " PRIORITY=" + priority);
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
        if (type.equals(AlgorithmType.RANDOM_FOREST)) {
            model.setTable(commandParameters.getEventTable());
        } else {            
            model.setTable(commandParameters.getDepivotedEventTable());
        }
        model.setMetadataTable(commandParameters.getMetadataTable());
        model.setTargetsList(commandParameters.getModelTargets());
        model.setKeyCols(commandParameters.getKeyCols());
        model.setCustomer(customer);
        model.setDataFormat(AVRO);
               
        List<String> features = modelingService.getFeatures(model, false);
        model.setFeaturesList(features);
        
        return model;
    }
}
