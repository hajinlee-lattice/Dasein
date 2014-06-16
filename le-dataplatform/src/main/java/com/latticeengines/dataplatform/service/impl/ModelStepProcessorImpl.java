package com.latticeengines.dataplatform.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import com.latticeengines.dataplatform.entitymanager.EventOutgoingEntityMgr;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.service.ModelCommandLogService;
import com.latticeengines.dataplatform.service.ModelStepProcessor;
import com.latticeengines.dataplatform.service.impl.dlorchestration.ModelCommandParameters;
import com.latticeengines.domain.exposed.dataplatform.Algorithm;
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
    
    private static final Log log = LogFactory.getLog(ModelStepProcessorImpl.class);
    
    private static final char COMMA = ',';
    private static final String AVRO = "avro";
    static final String LR_SAMPLENAME_PREFIX = "sLR";
    static final String RF_SAMPLENAME_PREFIX = "sRF";
    
    @Autowired
    private ModelingService modelingService;
    
    @Autowired
    private Configuration yarnConfiguration;
    
    @Autowired
    private ModelCommandLogService modelCommandLogService;
    
    @Autowired
    private EventOutgoingEntityMgr eventOutgoingEntityMgr;

    @Value("${dataplatform.datasource.host}")
    private String dbHost;
    
    @Value("${dataplatform.datasource.port}")
    private int dbPort;
    
    @Value("${dataplatform.datasource.dbname}")
    private String dbName;
    
    @Value("${dataplatform.datasource.user}")
    private String dbUser;
    
    @Value("${dataplatform.datasource.password}")
    private String dbPassword;
   
    @Value("${dataplatform.datasource.type}")
    private String dbType;
    
    @Value("${dataplatform.container.virtualcores}")
    private int virtualCores;
    
    @Value("${dataplatform.container.memory}")
    private int memory;
    
    @Override
    public void executeJsonStep(String deploymentExternalId, int modelCommandId, List<ModelCommandParameter> commandParameters) {        
        // TODO drop and create the new EvntTable schema.  write out HDFS paths
        // send compressed JSON to BARD, add some new LedpCode JSON error messages here
        // ex: /user/s-analytics/customers/Nutanix/models/Q_EventTableDepivot/dc93e0d7-ef30-43c5-8e7c-c8adab587f9f/1401731761443_1074/model.json
        // retry REST request as necessary, don't load complete JSON in memory
    }
    
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
            case ModelCommandParameters.METADATA_TABLE:
                modelCommandParameters.setMetadataTable(parameter.getValue());
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
            }
        }
        
        List<String> missingParameters = new ArrayList<>();
        if (Strings.isNullOrEmpty(modelCommandParameters.getDepivotedEventTable())) {
            missingParameters.add(ModelCommandParameters.DEPIVOTED_EVENT_TABLE);
        } 
        if (Strings.isNullOrEmpty(modelCommandParameters.getEventTable())) {
            missingParameters.add(ModelCommandParameters.EVENT_TABLE);
        } 
        if (modelCommandParameters.getKeyCols().isEmpty()) {
            missingParameters.add(ModelCommandParameters.KEY_COLS);
        } 
        if (Strings.isNullOrEmpty(modelCommandParameters.getMetadataTable())) {
            missingParameters.add(ModelCommandParameters.METADATA_TABLE);
        } 
        if (Strings.isNullOrEmpty(modelCommandParameters.getModelName())) {
            missingParameters.add(ModelCommandParameters.MODEL_NAME);
        }   
        if (modelCommandParameters.getModelTargets().isEmpty()) {
            missingParameters.add(ModelCommandParameters.MODEL_TARGETS);
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
        // No LR for now.
//        List<ApplicationId> depivotedAppIds = modelingService.loadData(generateLoadConfiguration(LR_SAMPLENAME_PREFIX, customer, commandParameters));
        
        appIds.add(modelingService.loadData(generateLoadConfiguration(RF_SAMPLENAME_PREFIX, customer, commandParameters)));
//        appIds.addAll(depivotedAppIds);
        
        return appIds;
    }
    
    private LoadConfiguration generateLoadConfiguration(String type, String customer, ModelCommandParameters commandParameters) {
        LoadConfiguration config = new LoadConfiguration();
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(dbHost).port(dbPort).db(dbName).user(dbUser).password(dbPassword).type(dbType);
        DbCreds creds = new DbCreds(builder);
        config.setCreds(creds);
        config.setCustomer(customer);
        if (type == LR_SAMPLENAME_PREFIX) {
            config.setTable(commandParameters.getDepivotedEventTable());
        } else {
            config.setTable(commandParameters.getEventTable());
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
//        ApplicationId lrAppId = modelingService.createSamples(generateSamplingConfiguration(LR_SAMPLENAME_PREFIX, customer, commandParameters));
        ApplicationId rfAppId = modelingService.createSamples(generateSamplingConfiguration(RF_SAMPLENAME_PREFIX, customer, commandParameters));

        return Arrays.asList(/*lrAppId, */rfAppId);
    }
  
    private String constructSampleName(String prefix, int percentage) {
        return prefix + String.valueOf(percentage);
    }
    
    @VisibleForTesting
    SamplingConfiguration generateSamplingConfiguration(String type, String customer, ModelCommandParameters commandParameters) {
        SamplingConfiguration samplingConfig = new SamplingConfiguration();
        samplingConfig.setTrainingPercentage(80);
        
        for (int percentage : calculateSamplePercentages(commandParameters.getNumSamples())) {
            SamplingElement s = new SamplingElement();
            s.setName(constructSampleName(type, percentage));
            s.setPercentage(percentage);
            samplingConfig.addSamplingElement(s);                 
        }       
        
        samplingConfig.setCustomer(customer);      
        
        if (type.equals(LR_SAMPLENAME_PREFIX)) {
            samplingConfig.setTable(commandParameters.getDepivotedEventTable());
        } else {
            samplingConfig.setTable(commandParameters.getEventTable());
        }
        
        return samplingConfig;
    }  
    
    private List<ApplicationId> submitModel(String customer, ModelCommandParameters commandParameters) {
        List<ApplicationId> appIds = new ArrayList<>();
        List<ApplicationId> rfAppIds = modelingService.submitModel(generateModel(RF_SAMPLENAME_PREFIX, customer, commandParameters));
        // No LR for now.
//        List<ApplicationId> lrAppIds = modelingService.submitModel(generateModel(LR_SAMPLENAME_PREFIX, customer, commandParameters));
        
        appIds.addAll(rfAppIds);
//        appIds.addAll(lrAppIds);
   
        return appIds;
    }
    
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
    Model generateModel(String type, String customer, ModelCommandParameters commandParameters) {
        List<Algorithm> algorithms = new ArrayList<>();
        
        int sampleIndex = 0;
        for (int percentage : calculateSamplePercentages(commandParameters.getNumSamples())) {
            AlgorithmBase algorithm;
            if (type.equals(LR_SAMPLENAME_PREFIX)) {
                algorithm = new LogisticRegressionAlgorithm();
            } else {
                algorithm = new RandomForestAlgorithm();
            }
                      
            int priority = calculatePriority(sampleIndex);
            algorithm.setPriority(calculatePriority(sampleIndex));
            algorithm.setContainerProperties("VIRTUALCORES=" + virtualCores + " MEMORY=" + memory + " PRIORITY=" + priority);
            algorithm.setSampleName(constructSampleName(type, percentage)); 
            algorithms.add(algorithm);
            
            sampleIndex++;
        }
        
        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName(commandParameters.getModelName());
        modelDef.setAlgorithms(algorithms);
        
        Model model = new Model();
        model.setModelDefinition(modelDef);
        model.setName(commandParameters.getModelName());
        if (type.equals(LR_SAMPLENAME_PREFIX)) {
            model.setTable(commandParameters.getDepivotedEventTable());
        } else {
            model.setTable(commandParameters.getEventTable());
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
