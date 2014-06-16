package com.latticeengines.dataplatform.exposed.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.dataplatform.client.yarn.AppMasterProperty;
import com.latticeengines.dataplatform.client.yarn.ContainerProperty;
import com.latticeengines.dataplatform.entitymanager.ModelEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ThrottleConfigurationEntityMgr;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.fairscheduler.LedpQueueAssigner;
import com.latticeengines.dataplatform.runtime.mapreduce.EventDataSamplingProperty;
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.domain.exposed.dataplatform.Algorithm;
import com.latticeengines.domain.exposed.dataplatform.Classifier;
import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.dataplatform.LoadConfiguration;
import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.domain.exposed.dataplatform.ModelDefinition;
import com.latticeengines.domain.exposed.dataplatform.SamplingConfiguration;
import com.latticeengines.domain.exposed.dataplatform.ThrottleConfiguration;
import com.latticeengines.domain.exposed.dataplatform.algorithm.DataProfilingAlgorithm;

@Component("modelingService")
public class ModelingServiceImpl implements ModelingService {
    
    private static final Log log = LogFactory.getLog(ModelingServiceImpl.class);
    
    @Autowired
    private Configuration yarnConfiguration;
    
    @Autowired
    private JobService jobService;
    
    @Autowired
    private ModelEntityMgr modelEntityMgr;

    @Autowired
    private ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr;

    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    /**
     * @param model
     * required attributes: job, modeldefinition - it should be something predefined.
     */
    public List<ApplicationId> submitModel(Model model) {
        setupModelProperties(model);
        List<ApplicationId> applicationIds = new ArrayList<ApplicationId>();
        ModelDefinition modelDefinition = model.getModelDefinition();

        List<Algorithm> algorithms = new ArrayList<Algorithm>();

        if (modelDefinition != null) {
            algorithms = modelDefinition.getAlgorithms();
        } else {
            throw new LedpException(LedpCode.LEDP_12005);
        }

        Collections.sort(algorithms, new Comparator<Algorithm>() {
            @Override
            public int compare(Algorithm o1, Algorithm o2) {
                return o1.getPriority() - o2.getPriority();
            }

        });
        ThrottleConfiguration config = throttleConfigurationEntityMgr.getLatestConfig();

        for (int i = 1; i <= algorithms.size(); i++) {
            Algorithm algorithm = algorithms.get(i - 1);

            if (doThrottling(config, algorithm, i)) {
                continue;
            }

            Job job = createJob(model, algorithm);
            model.addJob(job);
            // JobService is responsible for persistence during submitJob
            applicationIds.add(jobService.submitJob(job));
        }
        
        return applicationIds;
    }

    private void setupModelProperties(Model model) {
        model.setId(UUID.randomUUID().toString());
        model.setModelHdfsDir(customerBaseDir + "/" + model.getCustomer() + "/models/" + model.getTable() + "/"
                + model.getId());
        model.setDataHdfsPath(customerBaseDir + "/" + model.getCustomer() + "/data/" + model.getTable());
        model.setMetadataHdfsPath(model.getDataHdfsPath() + "/" + model.getMetadataTable());
    }

    private Classifier createClassifier(Model model, Algorithm algorithm) {
        Classifier classifier = new Classifier();

        classifier.setModelHdfsDir(model.getModelHdfsDir());
        classifier.setFeatures(model.getFeaturesList());
        classifier.setTargets(model.getTargetsList());
        classifier.setKeyCols(model.getKeyColsList());
        classifier.setPythonScriptHdfsPath(algorithm.getScript());
        classifier.setDataFormat(model.getDataFormat());
        classifier.setAlgorithmProperties(algorithm.getAlgorithmProperties());
        classifier.setMetadataHdfsPath(getAvroMetadataPathInHdfs(model.getMetadataHdfsPath()));

        String samplePrefix = algorithm.getSampleName();
        String trainingPath = getAvroFileHdfsPath(samplePrefix + "Training", model.getSampleHdfsPath());

        if (trainingPath == null) {
            throw new LedpException(LedpCode.LEDP_15001, new String[] { samplePrefix + "Training" });
        }
        String testPath = getAvroFileHdfsPath(samplePrefix + "Test", model.getSampleHdfsPath());
        if (testPath == null) {
            throw new LedpException(LedpCode.LEDP_15001, new String[] { samplePrefix + "Test" });
        }
        classifier.setTrainingDataHdfsPath(trainingPath);
        classifier.setTestDataHdfsPath(testPath);
        classifier.setSchemaHdfsPath(createSchemaInHdfs(trainingPath, model));
        return classifier;
    }
    
    private String getAvroMetadataPathInHdfs(String path) {
        List<String> files = new ArrayList<String>();
        try {
            files = HdfsUtils.getFilesForDir(yarnConfiguration, path, new HdfsFilenameFilter() {

                @Override
                public boolean accept(Path filename) {
                    return filename.toString().endsWith(".avro");
                }

            });
        } catch (Exception e) {
            log.warn(e);
        }
        
        if (files.size() != 1) {
            log.warn("No metadata file found.");
            return path;
        }
        String p = files.get(0);
        return p.substring(p.indexOf(customerBaseDir));
    }

    private String createSchemaInHdfs(String avroFilePath, Model model) {
        String dataPath = model.getDataHdfsPath();
        try {
            Schema avroSchema = AvroUtils.getSchema(yarnConfiguration, new Path(avroFilePath));
            dataPath += "/" + model.getTable() + ".avsc";
            if (!HdfsUtils.fileExists(yarnConfiguration, dataPath)) {
                HdfsUtils.writeToFile(yarnConfiguration, dataPath, avroSchema.toString(true));
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_15000, e);
        }
        return dataPath;
    }

    private String getAvroFileHdfsPath(final String samplePrefix, String baseDir) {
        List<String> files = new ArrayList<String>();
        try {
            files = HdfsUtils.getFilesForDir(yarnConfiguration, baseDir, new HdfsFilenameFilter() {

                @Override
                public boolean accept(Path filename) {
                    Pattern p = Pattern.compile(".*" + samplePrefix + ".*.avro");
                    Matcher matcher = p.matcher(filename.toString());
                    return matcher.matches();
                }

            });
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }

        if (files.size() == 1) {
            String path = files.get(0);
            return path.substring(path.indexOf(customerBaseDir));
        }
        return null;
    }
    
    private Job createJob(Model model, Algorithm algorithm) {
        String assignedQueue = LedpQueueAssigner.getNonMRQueueNameForSubmission(algorithm.getPriority());
        return createJob(model, algorithm, assignedQueue);
    }

    private Job createJob(Model model, Algorithm algorithm, String assignedQueue) {
        Job job = new Job();
        Classifier classifier = createClassifier(model, algorithm);
        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), model.getCustomer());
        appMasterProperties.put(AppMasterProperty.TABLE.name(), model.getTable());
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), assignedQueue);
        Properties containerProperties = algorithm.getContainerProps();
        containerProperties.put(ContainerProperty.METADATA.name(), classifier.toString());
        job.setClient("pythonClient");
        job.setAppMasterPropertiesObject(appMasterProperties);
        job.setContainerPropertiesObject(containerProperties);
        return job;
    }

    boolean doThrottling(ThrottleConfiguration config, Algorithm algorithm, int index) {
        if (config == null || !config.isEnabled()) {
            return false;
        }

        return index >= config.getJobRankCutoff();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void throttle(ThrottleConfiguration config) {
        config.setTimestampLong(System.currentTimeMillis());
        throttleConfigurationEntityMgr.create(config);
    }
    
    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void resetThrottle() {
        throttleConfigurationEntityMgr.deleteAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public ApplicationId createSamples(SamplingConfiguration config) {
        Model model = new Model();
        model.setCustomer(config.getCustomer());
        model.setTable(config.getTable());
        setupModelProperties(model);
        String inputDir = model.getDataHdfsPath();
        String outputDir = model.getSampleHdfsPath();
        Properties properties = new Properties();
        properties.setProperty(EventDataSamplingProperty.INPUT.name(), inputDir);
        properties.setProperty(EventDataSamplingProperty.OUTPUT.name(), outputDir);
        properties.setProperty(EventDataSamplingProperty.SAMPLE_CONFIG.name(), config.toString());
        properties.setProperty(EventDataSamplingProperty.CUSTOMER.name(), model.getCustomer());
        String assignedQueue = LedpQueueAssigner.getMRQueueNameForSubmission();
        properties.setProperty(EventDataSamplingProperty.QUEUE.name(), assignedQueue);
        return jobService.submitMRJob("samplingJob", properties);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public ApplicationId createFeatures(Model model, Set<String> excludeList) {
        if (model.getCustomer() == null) {
            throw new LedpException(LedpCode.LEDP_15002);
        }
        Model m = new Model();
        
        try {
            BeanUtils.copyProperties(m, model);
            m.setPid(null);
        } catch (Exception e) {
        }
        setupModelProperties(m);
        try {
            List<String> paths = HdfsUtils.getFilesForDir(yarnConfiguration, m.getDataHdfsPath() + "/samples",
                    new HdfsFilenameFilter() {

                @Override
                public boolean accept(Path filename) {
                    Pattern p = Pattern.compile(".*.avro");
                    Matcher matcher = p.matcher(filename.toString());
                    return matcher.matches();
                }

            });
            
            if (paths.size() == 0) {
                throw new LedpException(LedpCode.LEDP_00002);
            }
            String schemaPath = paths.get(0);
            List<String> featureList = new ArrayList<String>();
            Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(schemaPath));
            for (Field field : schema.getFields()) {
                String name = field.name();
                if (!excludeList.contains(name)) {
                    featureList.add(name);
                }
            }
            m.setFeaturesList(featureList);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
        m.setModelHdfsDir(m.getMetadataHdfsPath());
        ModelDefinition modelDefinition = new ModelDefinition();
        Algorithm dataProfileAlgorithm = new DataProfilingAlgorithm();
        dataProfileAlgorithm.setSampleName("all");
        dataProfileAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=64 PRIORITY=1");
        modelDefinition.addAlgorithms(Arrays.<Algorithm>asList(new Algorithm[] { dataProfileAlgorithm }));
        String assignedQueue = LedpQueueAssigner.getMRQueueNameForSubmission();
        Job job = createJob(m, dataProfileAlgorithm, assignedQueue);
        m.addJob(job);
        return jobService.submitJob(job);
    }
    
    @Override
    public List<String> getFeatures(Model model, boolean depivoted) {
        if (model.getCustomer() == null) {
            throw new LedpException(LedpCode.LEDP_15002);
        }
        setupModelProperties(model);
        String dataSchemaPath = model.getDataHdfsPath();
        String metadataPath = model.getMetadataHdfsPath();
        Schema dataSchema = null;
        List<GenericRecord> data = new ArrayList<GenericRecord>();
        List<String> features = new ArrayList<String>();
        Set<String> pivotedFeatures = new LinkedHashSet<>();
        
        try {
            HdfsFilenameFilter filter = new HdfsFilenameFilter() {

                @Override
                public boolean accept(Path path) {
                    return path.toString().endsWith(".avro");
                }
                
            };
            List<String> avroDataFiles = HdfsUtils.getFilesForDir(yarnConfiguration, dataSchemaPath, filter);
            
            if (avroDataFiles.size() == 0) {
                throw new LedpException(LedpCode.LEDP_15003, new String[] { "avro" });
            }
            
            dataSchema = AvroUtils.getSchema(yarnConfiguration, new Path(avroDataFiles.get(0)));

            List<String> avroMetadataFiles = HdfsUtils.getFilesForDir(yarnConfiguration, metadataPath, filter);
            
            if (avroMetadataFiles.size() == 0) {
                throw new LedpException(LedpCode.LEDP_15003, new String[] { "avro" });
            }
            
            for (String avroMetadataFile : avroMetadataFiles) {
                data.addAll(AvroUtils.getData(yarnConfiguration, new Path(avroMetadataFile)));
            }
            
            Set<String> columnSet = new HashSet<String>();
            Set<String> featureSet = new HashSet<String>();
            for (Field field : dataSchema.getFields()) {
                columnSet.add(field.getProp("columnName"));
            }            
            
            for (GenericRecord datum : data) {
                String name = datum.get("barecolumnname").toString();
                if (!depivoted) {
                    pivotedFeatures.add(name);
                    continue;
                }
                               
                String value = datum.get("columnvalue").toString();
                String datatype = datum.get("Dtype").toString();
                String featureName = name;
                
                if (featureName.equals("P1_Event")) {
                    continue;
                }
                
                if (datatype.equals("BND")) {
                    featureName += "_Continuous";
                } else {
                    featureName += "_" + value;
                }
                if (columnSet.contains(featureName) && !featureSet.contains(featureName)) {
                    features.add(featureName);
                    featureSet.add(featureName);
                }
            }
            
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
        
        if (depivoted) {
            return new ArrayList<String>(features);
        } else {
            return new ArrayList<String>(pivotedFeatures);
        }
    }

    @Override
    public ApplicationId loadData(LoadConfiguration config) {
        Model model = new Model();
        model.setCustomer(config.getCustomer());
        model.setTable(config.getTable());
        model.setMetadataTable(config.getMetadataTable());
        setupModelProperties(model);
        String assignedQueue = LedpQueueAssigner.getMRQueueNameForSubmission();
        return jobService.loadData(model.getTable(), model.getDataHdfsPath(),
                config.getCreds(), assignedQueue, model.getCustomer(), config.getKeyCols());
    }

    @Override
    public JobStatus getJobStatus(String applicationId) {
        return jobService.getJobStatus(applicationId);
    }
}
