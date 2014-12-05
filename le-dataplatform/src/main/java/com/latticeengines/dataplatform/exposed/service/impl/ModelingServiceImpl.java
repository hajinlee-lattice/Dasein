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
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.dataplatform.entitymanager.modeling.ModelEntityMgr;
import com.latticeengines.dataplatform.entitymanager.modeling.ThrottleConfigurationEntityMgr;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.exposed.yarn.client.AppMasterProperty;
import com.latticeengines.dataplatform.exposed.yarn.client.ContainerProperty;
import com.latticeengines.dataplatform.runtime.mapreduce.EventDataSamplingProperty;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.Algorithm;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;
import com.latticeengines.domain.exposed.modeling.ModelingJob;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.ThrottleConfiguration;
import com.latticeengines.domain.exposed.modeling.algorithm.AlgorithmBase;
import com.latticeengines.domain.exposed.modeling.algorithm.DataProfilingAlgorithm;
import com.latticeengines.scheduler.exposed.fairscheduler.LedpQueueAssigner;

@Component("modelingService")
public class ModelingServiceImpl implements ModelingService {

    private static final Log log = LogFactory.getLog(ModelingServiceImpl.class);

    private static final String DIAGNOSTIC_FILE = "diagnostics.json";

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ModelingJobService modelingJobService;

    @Autowired
    private ModelEntityMgr modelEntityMgr;

    @Autowired
    private ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr;

    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;

    @Value("${dataplatform.modeling.row.threshold:50}")
    private int rowSizeThreshold;
    
    @Value("${dataplatform.container.virtualcores}")
    private int virtualCores;

    @Value("${dataplatform.container.memory}")
    private int memory;
    
    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    /**
     * @param model
     * required attributes: job, modeldefinition - it should be something predefined.
     */
    public List<ApplicationId> submitModel(Model model) {
        setupModelProperties(model);
        try {
            validateModelInputData(model);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_15006, e);
        }
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

            ModelingJob modelingJob = createJob(model, algorithm);
            model.addModelingJob(modelingJob);
            // JobService is responsible for persistence during submitJob
            applicationIds.add(modelingJobService.submitJob(modelingJob));
        }

        return applicationIds;
    }

    private void setupModelProperties(Model model) {
        model.setId(UUID.randomUUID().toString());
        model.setModelHdfsDir(customerBaseDir + "/" + model.getCustomer() + "/models/" + model.getTable() + "/"
                + model.getId());
        model.setDataHdfsPath(customerBaseDir + "/" + model.getCustomer() + "/data/" + model.getTable());
        model.setMetadataHdfsPath(customerBaseDir + "/" + model.getCustomer() + "/data/" + model.getMetadataTable());
    }

    private void validateModelInputData(Model model) throws Exception {
        String diagnosticsPath = model.getMetadataHdfsPath() + "/" + DIAGNOSTIC_FILE;

        if (!HdfsUtils.fileExists(yarnConfiguration, diagnosticsPath)) {
            throw new LedpException(LedpCode.LEDP_15004);
        }
        // Parse diagnostics file
        String content = HdfsUtils.getHdfsFileContents(yarnConfiguration, diagnosticsPath);
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(content);
        long  sampleSize = (long) ((JSONObject) jsonObject.get("Summary")).get("SampleSize");

        if (sampleSize < rowSizeThreshold) {
            throw new LedpException(LedpCode.LEDP_15005, new String[] { Double.toString(sampleSize) });
        }

        return;
    }

    private Classifier createClassifier(Model model, Algorithm algorithm) {
        Classifier classifier = new Classifier();
        String modelName = model.getName();

        if (modelName != null) {
            classifier.setName(modelName.replace(' ', '_'));
        }

        classifier.setModelHdfsDir(model.getModelHdfsDir());
        classifier.setFeatures(model.getFeaturesList());
        classifier.setTargets(model.getTargetsList());
        classifier.setKeyCols(model.getKeyColsList());
        classifier.setPythonScriptHdfsPath(algorithm.getScript());

        String pipelineLibScript = algorithm.getPipelineLibScript();
        if (StringUtils.isEmpty(pipelineLibScript)) {
            pipelineLibScript = "/app/dataplatform/scripts/lepipeline.tar.gz";
        }
        String pipelineScript = algorithm.getPipelineScript();
        if (StringUtils.isEmpty(pipelineScript)) {
            pipelineScript = "/app/dataplatform/scripts/pipeline.py";
        }

        classifier.setPythonPipelineLibHdfsPath(pipelineLibScript);
        classifier.setPythonPipelineScriptHdfsPath(pipelineScript);
        classifier.setDataFormat(model.getDataFormat());
        classifier.setAlgorithmProperties(algorithm.getAlgorithmProperties());
        classifier.setProvenanceProperties(model.getProvenanceProperties());
        classifier.setDataProfileHdfsPath(getDataProfileAvroPathInHdfs(model.getMetadataHdfsPath()));
        classifier.setConfigMetadataHdfsPath(getConfigMetadataPathInHdfs(model.getMetadataHdfsPath()));
        classifier.setDataDiagnosticsPath(model.getMetadataHdfsPath() + "/" + DIAGNOSTIC_FILE);

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

    private String getDataProfileAvroPathInHdfs(String path) {
        List<String> files = new ArrayList<String>();
        try {
            files = HdfsUtils.getFilesForDir(yarnConfiguration, path, new HdfsFilenameFilter() {

                @Override
                public boolean accept(String filename) {
                    return filename.endsWith(".avro");
                }

            });
        } catch (Exception e) {
            log.warn(e);
        }

        if (files.size() != 1) {
            log.warn("No data profile found.");
            return path;
        }
        String p = files.get(0);
        return p.substring(p.indexOf(customerBaseDir));
    }

    private String getConfigMetadataPathInHdfs(String path) {
        List<String> files = new ArrayList<String>();
        try {
            files = HdfsUtils.getFilesForDir(yarnConfiguration, path, new HdfsFilenameFilter() {

                @Override
                public boolean accept(String filename) {
                    return filename.endsWith(".avsc");
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
                public boolean accept(String filename) {
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

    private ModelingJob createJob(Model model, Algorithm algorithm) {
        String assignedQueue = LedpQueueAssigner.getNonMRQueueNameForSubmission(algorithm.getPriority());
        return createJob(model, algorithm, assignedQueue);
    }

    private ModelingJob createJob(Model model, Algorithm algorithm, String assignedQueue) {
        ModelingJob modelingJob = new ModelingJob();
        Classifier classifier = createClassifier(model, algorithm);
        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), model.getCustomer());

        appMasterProperties.put(AppMasterProperty.QUEUE.name(), assignedQueue);
        Properties containerProperties = algorithm.getContainerProps();
        containerProperties.put(ContainerProperty.METADATA.name(), classifier.toString());
        // containerProperties.put(PythonContainerProperty.TABLE.name(),
        // model.getTable());
        modelingJob.setClient("pythonClient");
        modelingJob.setAppMasterPropertiesObject(appMasterProperties);
        modelingJob.setContainerPropertiesObject(containerProperties);
        return modelingJob;
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
        return modelingJobService.submitMRJob("samplingJob", properties);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public ApplicationId profileData(DataProfileConfiguration dataProfileConfig) {
        Set<String> excludeList = new HashSet<String>(dataProfileConfig.getExcludeColumnList());
        Set<String> includeList = new HashSet<String>(dataProfileConfig.getIncludeColumnList());
        if (dataProfileConfig.getCustomer() == null) {
            throw new LedpException(LedpCode.LEDP_15002);
        }
        Model m = new Model();
        m.setName("DataProfile-" + System.currentTimeMillis());
        m.setCustomer(dataProfileConfig.getCustomer());
        m.setTable(dataProfileConfig.getTable());
        m.setMetadataTable(dataProfileConfig.getMetadataTable());
        setupModelProperties(m);
        try {
            List<String> paths = HdfsUtils.getFilesForDir(yarnConfiguration, m.getDataHdfsPath() + "/samples",
                    new HdfsFilenameFilter() {

                        @Override
                        public boolean accept(String filename) {
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
            boolean useIncludeList = includeList.size() > 0;
            for (Field field : schema.getFields()) {
                String name = field.name();
                // If an include list is passed, only use the features in the
                // include list
                // if the name is part of the schema. If the include list is
                // empty, then
                // just add all the columns in the schema except for any columns
                // in the excluded list
                if (useIncludeList) {
                    if (includeList.contains(name)) {
                        featureList.add(name);
                    }
                } else {
                    if (!excludeList.contains(name)) {
                        featureList.add(name);
                    }
                }
            }
            m.setDataFormat("avro");
            m.setTargetsList(dataProfileConfig.getTargets());
            m.setKeyCols(Arrays.<String> asList(new String[] { featureList.get(0) }));
            m.setFeaturesList(featureList);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
        m.setModelHdfsDir(m.getMetadataHdfsPath());
        ModelDefinition modelDefinition = new ModelDefinition();
        modelDefinition.setName(m.getName());
        AlgorithmBase dataProfileAlgorithm = new DataProfilingAlgorithm();
        dataProfileAlgorithm.setSampleName(dataProfileConfig.getSamplePrefix());
        dataProfileAlgorithm.setContainerProperties("VIRTUALCORES=" + virtualCores + " MEMORY=" + memory + " PRIORITY=0");
        if (!StringUtils.isEmpty(dataProfileConfig.getScript())) {
            dataProfileAlgorithm.setScript(dataProfileConfig.getScript());
        }
        modelDefinition.addAlgorithms(Arrays.<Algorithm> asList(new Algorithm[] { dataProfileAlgorithm }));
        String assignedQueue = LedpQueueAssigner.getMRQueueNameForSubmission();
        m.setModelDefinition(modelDefinition);
        ModelingJob modelingJob = createJob(m, dataProfileAlgorithm, assignedQueue);
        m.addModelingJob(modelingJob);
        return modelingJobService.submitJob(modelingJob);
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
                public boolean accept(String path) {
                    return path.endsWith(".avro");
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

            Set<String> targetsSet = new HashSet<>();
            if (model.getTargetsList() != null) {
                targetsSet.addAll(model.getTargetsList());
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

                if (targetsSet.contains(featureName)) {
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
        return modelingJobService.loadData(model.getTable(), model.getDataHdfsPath(), config.getCreds(), assignedQueue,
                model.getCustomer(), config.getKeyCols());
    }

    @Override
    public JobStatus getJobStatus(String applicationId) {
        return modelingJobService.getJobStatus(applicationId);
    }
}
