package com.latticeengines.dataplatform.service.impl;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Resource;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
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

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFormat;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dataplatform.entitymanager.modeling.ThrottleConfigurationEntityMgr;
import com.latticeengines.dataplatform.exposed.mapreduce.MRJobUtil;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.dataplatform.exposed.yarn.client.AppMasterProperty;
import com.latticeengines.dataplatform.exposed.yarn.client.ContainerProperty;
import com.latticeengines.dataplatform.runtime.load.LoadProperty;
import com.latticeengines.dataplatform.runtime.mapreduce.sampling.EventDataSamplingProperty;
import com.latticeengines.dataplatform.service.DispatchService;
import com.latticeengines.dataplatform.service.ModelValidationService;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.Algorithm;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.DataReviewConfiguration;
import com.latticeengines.domain.exposed.modeling.DataSchema;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.ExportConfiguration;
import com.latticeengines.domain.exposed.modeling.Field;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;
import com.latticeengines.domain.exposed.modeling.ModelingJob;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.ThrottleConfiguration;
import com.latticeengines.domain.exposed.modeling.algorithm.AlgorithmBase;
import com.latticeengines.domain.exposed.modeling.algorithm.DataProfilingAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.DataReviewAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component("modelingService")
public class ModelingServiceImpl implements ModelingService {

    private static final Log log = LogFactory.getLog(ModelingServiceImpl.class);

    private static final String DIAGNOSTIC_FILE = "diagnostics.json";

    @Autowired
    private Configuration yarnConfiguration;

    @Resource(name = "modelingJobService")
    private ModelingJobService modelingJobService;

    @Autowired
    private ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr;

    @Autowired
    private SqoopSyncJobService sqoopSyncJobService;

    @Autowired
    private MetadataService metadataService;

    @Autowired
    private VersionManager versionManager;

    @Resource(name = "parallelDispatchService")
    private DispatchService dispatchService;

    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;

    @Value("${dataplatform.container.virtualcores}")
    private int virtualCores;

    @Value("${dataplatform.container.memory}")
    private int memory;

    @Value("${dataplatform.hdfs.stack:}")
    private String stackName;

    @Override
    public ApplicationId loadData(LoadConfiguration config) {
        Model model = new Model();
        model.setCustomer(config.getCustomer());
        model.setTable(config.getTable());
        model.setMetadataTable(config.getMetadataTable());
        setupModelProperties(model);
        String assignedQueue = LedpQueueAssigner.getModelingQueueNameForSubmission();

        String targetDir = config.getTargetHdfsDir();

        if (targetDir == null) {
            targetDir = model.getDataHdfsPath();
        }

        if (config.getQuery() != null) {
            return sqoopSyncJobService.importDataForQuery(config.getQuery(), //
                    targetDir, //
                    config.getCreds(), //
                    assignedQueue, //
                    model.getCustomer(), //
                    config.getKeyCols(), //
                    null);
        }
        return sqoopSyncJobService.importData(model.getTable(), targetDir, config.getCreds(), assignedQueue,
                model.getCustomer(), config.getKeyCols(),
                columnsToInclude(model.getTable(), config.getCreds(), config.getProperties()));
    }

    @Override
    public ApplicationId exportData(ExportConfiguration config) {
        String assignedQueue = LedpQueueAssigner.getModelingQueueNameForSubmission();

        return sqoopSyncJobService.exportData(config.getTable(), config.getHdfsDirPath(), config.getCreds(),
                assignedQueue, config.getCustomer());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public ApplicationId createSamples(SamplingConfiguration config) {

        dispatchService.customizeSampleConfig(config, config.isParallelEnabled());

        Model model = new Model();
        model.setCustomer(config.getCustomer());
        model.setTable(config.getTable());
        setupModelProperties(model);
        String inputDir = model.getDataHdfsPath();

        if (config.getHdfsDirPath() != null) {
            inputDir = config.getHdfsDirPath();
        }
        String outputDir = model.getSampleHdfsPath();
        Properties properties = new Properties();
        properties.setProperty(MapReduceProperty.INPUT.name(), inputDir);
        properties.setProperty(MapReduceProperty.OUTPUT.name(), outputDir);
        properties.setProperty(EventDataSamplingProperty.SAMPLE_CONFIG.name(), config.toString());
        properties.setProperty(MapReduceProperty.CUSTOMER.name(), model.getCustomer());
        String assignedQueue = LedpQueueAssigner.getModelingQueueNameForSubmission();
        properties.setProperty(MapReduceProperty.QUEUE.name(), assignedQueue);
        properties.setProperty(
                MapReduceProperty.CACHE_FILE_PATH.name(),
                MRJobUtil.getPlatformShadedJarPath(yarnConfiguration,
                        versionManager.getCurrentVersionInStack(stackName)));

        return modelingJobService.submitMRJob(dispatchService.getSampleJobName(config.isParallelEnabled()), properties);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public ApplicationId profileData(DataProfileConfiguration dataProfileConfig) {
        if (dataProfileConfig.getCustomer() == null) {
            throw new LedpException(LedpCode.LEDP_15002);
        }

        Model m = setupProfileModel(dataProfileConfig);
        List<String> featureList = getFeatureList(dataProfileConfig, m);
        if (featureList.isEmpty()) {
            throw new LedpException(LedpCode.LEDP_15016);
        }

        m.setDataFormat("avro");
        m.setTargetsList(dataProfileConfig.getTargets());
        m.setKeyCols(Arrays.<String> asList(new String[] { featureList.get(0) }));
        m.setFeaturesList(featureList);

        m.setModelHdfsDir(m.getMetadataHdfsPath());
        ModelDefinition modelDefinition = new ModelDefinition();
        modelDefinition.setName(m.getName());
        AlgorithmBase dataProfileAlgorithm = new DataProfilingAlgorithm();
        dataProfileAlgorithm.setSampleName(dataProfileConfig.getSamplePrefix());
        dataProfileAlgorithm.setMapperSize(dispatchService.getNumberOfProfilingMappers(dataProfileConfig
                .isParallelEnabled()));
        if (StringUtils.isEmpty(dataProfileConfig.getContainerProperties())) {
            dataProfileAlgorithm.setContainerProperties(getDefaultContainerProperties());
        } else {
            dataProfileAlgorithm.setContainerProperties(dataProfileConfig.getContainerProperties());
        }
        if (!StringUtils.isEmpty(dataProfileConfig.getScript())) {
            dataProfileAlgorithm.setScript(dataProfileConfig.getScript());
        }
        modelDefinition.addAlgorithms(Arrays.<Algorithm> asList(new Algorithm[] { dataProfileAlgorithm }));
        String assignedQueue = LedpQueueAssigner.getModelingQueueNameForSubmission();
        m.setModelDefinition(modelDefinition);

        ModelingJob modelingJob = createJob(m, dataProfileAlgorithm, assignedQueue,
                dispatchService.getProfileJobName(dataProfileConfig.isParallelEnabled()));
        m.addModelingJob(modelingJob);
        return dispatchService.submitJob(modelingJob, dataProfileConfig.isParallelEnabled(), false);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public ApplicationId reviewData(DataReviewConfiguration dataReviewConfig) {
        if (dataReviewConfig.getCustomer() == null) {
            throw new LedpException(LedpCode.LEDP_15002);
        }

        Model m = setupProfileModel(dataReviewConfig);
        m.setName("DataReview-" + System.currentTimeMillis());
        List<String> featureList = getFeatureList(dataReviewConfig, m);
        if (featureList.isEmpty()) {
            throw new LedpException(LedpCode.LEDP_15016);
        }

        m.setDataFormat("avro");
        m.setTargetsList(dataReviewConfig.getTargets());
        m.setKeyCols(Arrays.<String> asList(new String[] { featureList.get(0) }));
        m.setFeaturesList(featureList);

        m.setModelHdfsDir(m.getMetadataHdfsPath());
        ModelDefinition modelDefinition = new ModelDefinition();
        modelDefinition.setName(m.getName());
        AlgorithmBase dataReviewAlgorithm = new DataReviewAlgorithm();
        dataReviewAlgorithm.setSampleName(dataReviewConfig.getSamplePrefix());
        dataReviewAlgorithm.setMapperSize("0");
        if (StringUtils.isEmpty(dataReviewConfig.getContainerProperties())) {
            dataReviewAlgorithm.setContainerProperties(getDefaultContainerProperties());
        } else {
            dataReviewAlgorithm.setContainerProperties(dataReviewConfig.getContainerProperties());
        }
        if (!StringUtils.isEmpty(dataReviewConfig.getScript())) {
            dataReviewAlgorithm.setScript(dataReviewConfig.getScript());
        }
        modelDefinition.addAlgorithms(Arrays.<Algorithm> asList(new Algorithm[] { dataReviewAlgorithm }));
        String assignedQueue = LedpQueueAssigner.getModelingQueueNameForSubmission();
        m.setModelDefinition(modelDefinition);

        ModelingJob modelingJob = createJob(m, dataReviewAlgorithm, assignedQueue, "reviewing");
        m.addModelingJob(modelingJob);
        return dispatchService.submitJob(modelingJob, dataReviewConfig.isParallelEnabled(), false);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public List<ApplicationId> submitModel(Model model) {
        setupModelProperties(model);
        try {
            validateModelInputData(model);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_15006, e);
        }
        List<ApplicationId> applicationIds = new ArrayList<ApplicationId>();
        ModelDefinition modelDefinition = model.getModelDefinition();

        List<Algorithm> algorithms = null;

        if (modelDefinition != null) {
            algorithms = modelDefinition.getAlgorithms();
        } else {
            throw new LedpException(LedpCode.LEDP_12005);
        }

        algorithms = checkModelAndAlgorithm(model, algorithms);

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

            if (StringUtils.isEmpty(algorithm.getContainerProperties())) {
                algorithm.setContainerProperties(getDefaultContainerProperties());
            }
            algorithm.setMapperSize(dispatchService.getNumberOfSamplingTrainingSet(model.isParallelEnabled()));
            ModelingJob modelingJob = createJob(model, algorithm,
                    dispatchService.getModelingJobName(model.isParallelEnabled()));
            model.addModelingJob(modelingJob);

            // JobService is responsible for persistence during submitJob
            applicationIds.add(dispatchService.submitJob(modelingJob, model.isParallelEnabled(), true));
        }

        return applicationIds;
    }

    private List<Algorithm> checkModelAndAlgorithm(Model model, List<Algorithm> algorithms) {
        if (CollectionUtils.isEmpty(model.getFeaturesList())) {
            model.setFeaturesList(getFeatures(model, false));
        }
        if (CollectionUtils.isEmpty(algorithms)) {
            algorithms = new ArrayList<Algorithm>();
            RandomForestAlgorithm algorithm = new RandomForestAlgorithm();
            algorithm.resetAlgorithmProperties();
            algorithm.setSampleName("all");
            algorithm.setPriority(0);
            algorithm.setContainerProperties(getDefaultContainerProperties());
            algorithms.add(algorithm);
        }
        return algorithms;
    }

    private String getDefaultContainerProperties() {
        return "VIRTUALCORES=" + virtualCores + " MEMORY=" + memory + " PRIORITY=0";
    }

    private void setupModelProperties(Model model) {
        model.setId(UUID.randomUUID().toString());
        model.setModelHdfsDir(customerBaseDir + "/" + model.getCustomer() + "/models/" + model.getTable() + "/"
                + model.getId());
        model.setDataHdfsPath(customerBaseDir + "/" + model.getCustomer() + "/data/" + model.getTable());
        model.setMetadataHdfsPath(customerBaseDir + "/" + model.getCustomer() + "/data/" + model.getMetadataTable());
    }

    private Model setupProfileModel(DataProfileConfiguration dataProfileConfig) {
        Model m = new Model();
        m.setName("DataProfile-" + System.currentTimeMillis());
        m.setCustomer(dataProfileConfig.getCustomer());
        m.setTable(dataProfileConfig.getTable());
        m.setMetadataTable(dataProfileConfig.getMetadataTable());
        setupModelProperties(m);
        m.setParallelEnabled(dataProfileConfig.isParallelEnabled());
        return m;
    }

    private void validateModelInputData(Model model) throws Exception {
        try {
            ModelValidationService validator = ModelValidationService.get(model.getModelDefinition().getAlgorithms()
                    .get(0).getName());
            validator.validate(model);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
    }

    private void setDefaultValues(Algorithm algorithm) {
        RandomForestAlgorithm rf = new RandomForestAlgorithm();
        if (StringUtils.isEmpty(algorithm.getPipelineDriver())) {
            algorithm.setPipelineDriver(rf.getPipelineDriver());
        }
        if (StringUtils.isEmpty(algorithm.getPipelineScript())) {
            algorithm.setPipelineScript(rf.getPipelineScript());
        }
        if (StringUtils.isEmpty(algorithm.getPipelineLibScript())) {
            algorithm.setPipelineLibScript(rf.getPipelineLibScript());
        }
    }

    private Classifier createClassifier(Model model, Algorithm algorithm) {
        Classifier classifier = new Classifier();
        String modelName = model.getName();

        if (modelName != null) {
            classifier.setName(modelName.replace(' ', '_'));
        }
        if (model.getDisplayName() != null) {
            classifier.setDisplayName(model.getDisplayName());
        } else {
            classifier.setDisplayName(classifier.getName());
        }

        setDefaultValues(algorithm);

        classifier.setModelHdfsDir(model.getModelHdfsDir());
        classifier.setFeatures(model.getFeaturesList());
        classifier.setTargets(model.getTargetsList());
        classifier.setKeyCols(model.getKeyColsList());
        classifier.setPythonScriptHdfsPath(getScriptPathWithVersion(algorithm.getScript()));
        classifier.setPipelineDriver(getScriptPathWithVersion(algorithm.getPipelineDriver()));
        classifier.setPythonPipelineLibHdfsPath(getScriptPathWithVersion(algorithm.getPipelineLibScript()));
        classifier.setPythonPipelineScriptHdfsPath(getScriptPathWithVersion(algorithm.getPipelineScript()));
        classifier.setDataFormat(model.getDataFormat());
        classifier.setAlgorithmProperties(algorithm.getAlgorithmProperties());
        classifier.setProvenanceProperties(model.getProvenanceProperties());
        classifier.setPipelineProperties(algorithm.getPipelineProperties());
        classifier.setDataProfileHdfsPath(getDataProfileAvroPathInHdfs(model.getMetadataHdfsPath()));
        classifier.setConfigMetadataHdfsPath(getConfigMetadataPathInHdfs(model.getMetadataHdfsPath()));

        if (algorithm.hasDataDiagnostics()) {
            classifier.setDataDiagnosticsPath(model.getMetadataHdfsPath() + "/" + DIAGNOSTIC_FILE);
        }

        String samplePrefix = algorithm.getSampleName();

        String trainingFile = dispatchService.getTrainingFile(samplePrefix, model.isParallelEnabled());
        String trainingPath = getAvroFileHdfsPath(trainingFile, model.getSampleHdfsPath());

        if (trainingPath == null) {
            throw new LedpException(LedpCode.LEDP_15001, new String[] { trainingFile });
        }

        String testFile = dispatchService.getTestFile(samplePrefix, model.isParallelEnabled());
        String testPath = getAvroFileHdfsPath(testFile, model.getSampleHdfsPath());
        if (testPath == null) {
            throw new LedpException(LedpCode.LEDP_15001, new String[] { testFile });
        }
        classifier.setTrainingDataHdfsPath(trainingPath);
        classifier.setTestDataHdfsPath(testPath);

        classifier.setSchemaHdfsPath(createSchemaInHdfs(trainingPath, model));
        return classifier;
    }

    private String getScriptPathWithVersion(String script) {
        String afterPart = StringUtils.substringAfter(script, "/app");
        return "/app/" + versionManager.getCurrentVersionInStack(stackName) + afterPart;
    }

    private String getDataProfileAvroPathInHdfs(String path) {
        List<String> files = new ArrayList<String>();
        try {
            files = HdfsUtils.getFilesForDir(yarnConfiguration, path, HdfsFileFormat.AVRO_FILE);
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
            files = HdfsUtils.getFilesForDir(yarnConfiguration, path, HdfsFileFormat.AVSC_FILE);
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
        String schemaContents = model.getSchemaContents();
        if (schemaContents == null) {
            Schema avroSchema = AvroUtils.getSchema(yarnConfiguration, new Path(avroFilePath));
            schemaContents = avroSchema.toString(true);
        }
        try {
            dataPath += "/" + model.getTable() + ".avsc";
            if (!HdfsUtils.fileExists(yarnConfiguration, dataPath)) {
                HdfsUtils.writeToFile(yarnConfiguration, dataPath, schemaContents);
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_15000, e);
        }

        return dataPath;
    }

    private String getAvroFileHdfsPath(final String samplePrefix, String baseDir) {
        String format = ".*" + samplePrefix + HdfsFileFormat.AVRO_FILE;
        List<String> files = new ArrayList<String>();
        try {
            files = HdfsUtils.getFilesForDir(yarnConfiguration, baseDir, format);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }

        if (files.size() == 1) {
            String p = files.get(0);
            return p.substring(p.indexOf(customerBaseDir));
        }
        return null;
    }

    private ModelingJob createJob(Model model, Algorithm algorithm, String jobType) {
        String assignedQueue = LedpQueueAssigner.getModelingQueueNameForSubmission();
        return createJob(model, algorithm, assignedQueue, jobType);
    }

    private ModelingJob createJob(Model model, Algorithm algorithm, String assignedQueue, String jobType) {
        ModelingJob modelingJob = new ModelingJob();
        Classifier classifier = createClassifier(model, algorithm);
        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), model.getCustomer());
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), assignedQueue);
        appMasterProperties
                .put(dispatchService.getMapSizeKeyName(model.isParallelEnabled()), algorithm.getMapperSize());
        Properties containerProperties = algorithm.getContainerProps();
        containerProperties.put(ContainerProperty.METADATA.name(), classifier.toString());
        containerProperties.put(ContainerProperty.JOB_TYPE.name(), jobType);
        modelingJob.setClient("pythonClient");
        modelingJob.setCustomer(model.getCustomer());
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

    private List<String> getFeatureList(DataProfileConfiguration dataProfileConfig, Model m) {
        List<String> featureList = new ArrayList<String>();
        List<String> includeList = dataProfileConfig.getIncludeColumnList();
        List<String> excludeList = dataProfileConfig.getExcludeColumnList();
        List<String> eventList = getEventList(dataProfileConfig.getTargets());

        String sampleDataPath = m.getDataHdfsPath() + "/samples";
        String schemaPath = getSchemaPath(sampleDataPath);
        Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(schemaPath));

        boolean useIncludeList = CollectionUtils.isNotEmpty(includeList);
        for (org.apache.avro.Schema.Field field : schema.getFields()) {
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
                if (!excludeList.contains(name) && !eventList.contains(name)) {
                    featureList.add(name);
                }
            }
        }
        return featureList;
    }

    @Override
    public List<String> getFeatures(Model model, boolean depivoted) {
        if (model.getCustomer() == null) {
            throw new LedpException(LedpCode.LEDP_15002);
        }
        setupModelProperties(model);
        String sampleSchemaPath = model.getSampleHdfsPath();
        String metadataPath = model.getMetadataHdfsPath();
        Schema dataSchema = null;
        List<GenericRecord> data = new ArrayList<GenericRecord>();
        List<String> features = new ArrayList<String>();
        Set<String> pivotedFeatures = new LinkedHashSet<>();
        Map<String, Double> featureScoreMap = new HashMap<String, Double>();

        try {
            List<String> avroDataFiles = HdfsUtils.getFilesForDir(yarnConfiguration, sampleSchemaPath,
                    HdfsFileFormat.AVRO_FILE);
            if (avroDataFiles.size() == 0) {
                throw new LedpException(LedpCode.LEDP_15003, new String[] { "avro" });
            }
            dataSchema = AvroUtils.getSchema(yarnConfiguration, new Path(avroDataFiles.get(0)));

            List<String> avroMetadataFiles = HdfsUtils.getFilesForDir(yarnConfiguration, metadataPath,
                    HdfsFileFormat.AVRO_FILE);
            if (avroMetadataFiles.size() == 0) {
                throw new LedpException(LedpCode.LEDP_15003, new String[] { "avro" });
            }

            for (String avroMetadataFile : avroMetadataFiles) {
                data.addAll(AvroUtils.getData(yarnConfiguration, new Path(avroMetadataFile)));
            }

            Set<String> columnSet = new HashSet<String>();
            Set<String> featureSet = new HashSet<String>();

            for (org.apache.avro.Schema.Field field : dataSchema.getFields()) {
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
                    if (model.getFeaturesThreshold() > 0) {
                        String columnValue = "0";
                        if (datum.get("uncertaintyCoefficient") != null) {
                            columnValue = datum.get("uncertaintyCoefficient").toString();
                        }
                        populateFeatureScore(featureScoreMap, name, columnValue);
                    }
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
            if (model.getFeaturesThreshold() > 0) {
                return getSortedFeatureList(featureScoreMap, model.getFeaturesThreshold());
            } else {
                return new ArrayList<String>(pivotedFeatures);
            }
        }
    }

    List<String> getSortedFeatureList(Map<String, Double> featureScoreMap, int featuresTreshold) {
        List<Map.Entry<String, Double>> featureEntries = new ArrayList<>(featureScoreMap.entrySet());
        Collections.sort(featureEntries, new Comparator<Map.Entry<String, Double>>() {
            @Override
            public int compare(Entry<String, Double> e1, Entry<String, Double> e2) {
                return Double.compare(e2.getValue(), e1.getValue());
            }
        });

        List<String> features = new ArrayList<String>();
        for (Map.Entry<String, Double> featureEntry : featureEntries) {
            features.add(featureEntry.getKey());
        }
        if (features.size() > featuresTreshold) {
            log.info("The number of skipped features is=" + (features.size() - featuresTreshold));
            return features.subList(0, featuresTreshold);
        } else {
            return features;
        }
    }

    void populateFeatureScore(Map<String, Double> featureScoreMap, String columnName, String columnValue) {
        try {
            Double uc = featureScoreMap.get(columnName);
            if (uc == null) {
                featureScoreMap.put(columnName, Double.parseDouble(columnValue));
            } else {
                featureScoreMap.put(columnName, uc + Double.parseDouble(columnValue));
            }
        } catch (Exception ex) {
            log.warn("Failed to pupoluate feature score!", ex);
        }
    }

    private String getSchemaPath(String sampleDataPath) {
        String schemaPath = null;
        try {
            List<String> paths = HdfsUtils.getFilesForDir(yarnConfiguration, sampleDataPath, HdfsFileFormat.AVRO_FILE);
            if (CollectionUtils.isEmpty(paths)) {
                throw new LedpException(LedpCode.LEDP_15007, new String[] { sampleDataPath });
            }
            schemaPath = paths.get(0);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
        return schemaPath;
    }

    @VisibleForTesting
    List<String> getEventList(List<String> targets) {
        List<String> eventList = new ArrayList<String>();
        String eventKey = "Event:"; // assumes key:value form
        for (String token : targets) {
            int index = token.indexOf(eventKey);
            if (index >= 0) {
                // List of key-value pairs
                eventList.add(token.substring(index + eventKey.length() + 1));
                return eventList;
            }
        }

        // List of columns
        Set<String> eventSet = new HashSet<String>(targets);
        eventList.addAll(eventSet);

        return eventList;
    }

    private String columnsToInclude(String table, DbCreds creds, Map<String, String> properties) {
        StringBuilder lb = new StringBuilder();
        try {
            DataSchema dataSchema = metadataService.createDataSchema(creds, table);
            List<Field> fields = dataSchema.getFields();

            boolean excludeTimestampCols = Boolean.parseBoolean(LoadProperty.EXCLUDETIMESTAMPCOLUMNS
                    .getValue(properties));
            boolean first = true;
            for (Field field : fields) {

                // The scoring engine does not know how to convert datetime
                // columns into a numeric value,
                // which Sqoop does automatically. This should not be a problem
                // now since dates are
                // typically not predictive anyway so we can safely exclude them
                // for now.
                // We can start including TIMESTAMP and TIME columns by
                // explicitly setting EXCLUDETIMESTAMPCOLUMNS=false
                // in the load configuration.
                if (excludeTimestampCols && (field.getSqlType() == Types.TIMESTAMP || field.getSqlType() == Types.TIME)) {
                    continue;
                }
                String name = field.getName();
                String colName = field.getColumnName();

                if (name == null) {
                    log.warn("Field name is null.");
                    continue;
                }
                if (colName == null) {
                    log.warn("Column name is null.");
                    continue;
                }
                if (!first) {
                    lb.append(",");
                } else {
                    first = false;
                }
                lb.append(colName);
                if (!colName.equals(name)) {
                    log.warn(LedpException.buildMessageWithCode(LedpCode.LEDP_11005, new String[] { colName, name }));
                }
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_11004, e, new String[] { table });
        }
        return lb.toString();
    }

    @Override
    public JobStatus getJobStatus(String applicationId) {
        return modelingJobService.getJobStatus(applicationId);
    }
}
