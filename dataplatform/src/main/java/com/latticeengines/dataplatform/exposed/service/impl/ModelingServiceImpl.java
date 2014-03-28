package com.latticeengines.dataplatform.exposed.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.entitymanager.ModelEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ThrottleConfigurationEntityMgr;
import com.latticeengines.dataplatform.exposed.domain.Algorithm;
import com.latticeengines.dataplatform.exposed.domain.Classifier;
import com.latticeengines.dataplatform.exposed.domain.Job;
import com.latticeengines.dataplatform.exposed.domain.Model;
import com.latticeengines.dataplatform.exposed.domain.ModelDefinition;
import com.latticeengines.dataplatform.exposed.domain.ThrottleConfiguration;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.service.JobService;

@Component("modelingService")
public class ModelingServiceImpl implements ModelingService {

    @Autowired
    private JobService jobService;
    
    @Autowired
    private ModelEntityMgr modelEntityMgr;
    
    @Autowired
    private ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr;
    
    @Value("{dataplatform.customer.basedir}")
    private String customerBaseDir;

    @Override
    public void setupCustomer(String customerName) {
    	jobService.createHdfsDirectory(customerBaseDir + "/" + customerName + "/models", false);
    	jobService.createHdfsDirectory(customerBaseDir + "/" + customerName + "/data", false);
    }
    
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
    	model.setModelHdfsDir(customerBaseDir + "/" + model.getCustomer() + "/models/" + model.getTable() + "/" + model.getId());
    	model.setDataHdfsPath(customerBaseDir + "/" + model.getCustomer() + "/data/" + model.getTable() + "/samples");
    }
    
    private Classifier createClassifier(Model model, Algorithm algorithm) {
    	Classifier classifier = new Classifier();
        classifier.setSchemaHdfsPath(model.getSchemaHdfsPath());      
        classifier.setModelHdfsDir(model.getModelHdfsDir());
        classifier.setFeatures(model.getFeatures());
        classifier.setTargets(model.getTargets());
        classifier.setPythonScriptHdfsPath(algorithm.getScript());
        
        String sample = algorithm.getSampleName();
        
        classifier.setTrainingDataHdfsPath(model.getDataHdfsPath());
        classifier.setTestDataHdfsPath(model.getDataHdfsPath());
        return classifier;
    }
    
    private Job createJob(Model model, Algorithm algorithm) {
    	Job job = new Job();
    	Classifier classifier = createClassifier(model, algorithm);
        Properties appMasterProperties = new Properties();
        appMasterProperties.put("CUSTOMER", model.getCustomer());
        appMasterProperties.put("TABLE", model.getTable());
        appMasterProperties.put("QUEUE", "Priority" + algorithm.getPriority() + ".A");
        Properties containerProperties = algorithm.getContainerProps();
        containerProperties.put("METADATA", classifier.toString());
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

}
