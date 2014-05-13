package com.latticeengines.dataplatform.exposed.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.dataplatform.client.yarn.AppMasterProperty;
import com.latticeengines.dataplatform.client.yarn.ContainerProperty;
import com.latticeengines.dataplatform.entitymanager.ModelEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ThrottleConfigurationEntityMgr;
import com.latticeengines.dataplatform.exposed.domain.Algorithm;
import com.latticeengines.dataplatform.exposed.domain.Classifier;
import com.latticeengines.dataplatform.exposed.domain.Job;
import com.latticeengines.dataplatform.exposed.domain.JobStatus;
import com.latticeengines.dataplatform.exposed.domain.LoadConfiguration;
import com.latticeengines.dataplatform.exposed.domain.Model;
import com.latticeengines.dataplatform.exposed.domain.ModelDefinition;
import com.latticeengines.dataplatform.exposed.domain.SamplingConfiguration;
import com.latticeengines.dataplatform.exposed.domain.ThrottleConfiguration;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.runtime.mapreduce.EventDataSamplingProperty;
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.dataplatform.service.YarnQueueAssignmentService;

@Component("modelingService")
public class ModelingServiceImpl implements ModelingService {
    
    @Autowired
    private Configuration yarnConfiguration;
    
    @Autowired
    private JobService jobService;
    
    @Autowired
    private ModelEntityMgr modelEntityMgr;

    @Autowired
    private ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr;

    @Autowired
    private YarnQueueAssignmentService yarnQueueAssignmentService;
    
    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;

    @Override
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
            applicationIds.add(jobService.submitJob(job));
        }

        modelEntityMgr.post(model);
        modelEntityMgr.save();

        return applicationIds;
    }

    private void setupModelProperties(Model model) {
        model.setId(UUID.randomUUID().toString());
        model.setModelHdfsDir(customerBaseDir + "/" + model.getCustomer() + "/models/" + model.getTable() + "/"
                + model.getId());
        model.setDataHdfsPath(customerBaseDir + "/" + model.getCustomer() + "/data/" + model.getTable());
    }

    private Classifier createClassifier(Model model, Algorithm algorithm) {
        Classifier classifier = new Classifier();

        classifier.setModelHdfsDir(model.getModelHdfsDir());
        classifier.setFeatures(model.getFeatures());
        classifier.setTargets(model.getTargets());
        classifier.setKeyCols(model.getKeyCols());
        classifier.setPythonScriptHdfsPath(algorithm.getScript());
        classifier.setDataFormat(model.getDataFormat());
        classifier.setAlgorithmProperties(algorithm.getAlgorithmProperties());

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
        Job job = new Job();
        Classifier classifier = createClassifier(model, algorithm);
        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), model.getCustomer());
        appMasterProperties.put(AppMasterProperty.TABLE.name(), model.getTable());
        String assignedQueue = yarnQueueAssignmentService.getAssignedQueue(model.getCustomer(), "Priority" + algorithm.getPriority());
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), assignedQueue);
        Properties containerProperties = algorithm.getContainerProps();
        containerProperties.put(ContainerProperty.METADATA.name(), classifier.toString());
        job.setClient("pythonClient");
        job.setAppMasterProperties(appMasterProperties);
        job.setContainerProperties(containerProperties);
        return job;
    }

    boolean doThrottling(ThrottleConfiguration config, Algorithm algorithm, int index) {
        if (config == null || !config.isEnabled()) {
            return false;
        }

        return index >= config.getJobRankCutoff();
    }

    @Override
    public void throttle(ThrottleConfiguration config) {
        config.setTimestamp(System.currentTimeMillis());
        throttleConfigurationEntityMgr.post(config);
        throttleConfigurationEntityMgr.save();
    }

    @Override
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
        String assignedQueue = yarnQueueAssignmentService.getAssignedQueue(model.getCustomer(), "MapReduce");
        properties.setProperty(EventDataSamplingProperty.QUEUE.name(), assignedQueue);
        return jobService.submitMRJob("samplingJob", properties);
    }

    @Override
    public ApplicationId createFeatures(Model model) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ApplicationId loadData(LoadConfiguration config) {
        Model model = new Model();
        model.setCustomer(config.getCustomer());
        model.setTable(config.getTable());
        setupModelProperties(model);
        String assignedQueue = yarnQueueAssignmentService.getAssignedQueue(
                model.getCustomer(), "MapReduce");
        return jobService.loadData(model.getTable(), model.getDataHdfsPath(),
                config.getCreds(), assignedQueue, model.getCustomer(), config.getKeyCols());
    }

    @Override
    public JobStatus getJobStatus(String applicationId) {
        return jobService.getJobStatus(applicationId);
    }
}
