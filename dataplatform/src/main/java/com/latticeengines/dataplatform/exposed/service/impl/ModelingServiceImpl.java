package com.latticeengines.dataplatform.exposed.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.entitymanager.ModelEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ThrottleConfigurationEntityMgr;
import com.latticeengines.dataplatform.exposed.domain.Algorithm;
import com.latticeengines.dataplatform.exposed.domain.Classifier;
import com.latticeengines.dataplatform.exposed.domain.Job;
import com.latticeengines.dataplatform.exposed.domain.Model;
import com.latticeengines.dataplatform.exposed.domain.ModelDefinition;
import com.latticeengines.dataplatform.exposed.domain.ThrottleConfiguration;
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
    
    @Override
    public List<ApplicationId> submitModel(Model model) {
        List<ApplicationId> applicationIds = new ArrayList<ApplicationId>();
        ModelDefinition modelDefinition = model.getModelDefinition();

        List<Algorithm> algorithms = modelDefinition.getAlgorithms();
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
            Classifier classifier = new Classifier();
            classifier.setSchemaHdfsPath(model.getSchemaHdfsPath());
            classifier.setTrainingDataHdfsPath(model.getTrainingDataHdfsPath());
            classifier.setTestDataHdfsPath(model.getTestDataHdfsPath());
            classifier.setModelHdfsDir(model.getModelHdfsDir());
            classifier.setFeatures(model.getFeatures());
            classifier.setTargets(model.getTargets());
            classifier.setPythonScriptHdfsPath(algorithm.getScript());
            Properties appMasterProperties = new Properties();
            appMasterProperties.put("QUEUE", "Priority" + algorithm.getPriority() + ".A");
            Properties containerProperties = algorithm.getContainerProps();
            containerProperties.put("METADATA", classifier.toString());
            Job job = new Job();
            job.setClient("pythonClient");
            job.setAppMasterProperties(appMasterProperties);
            job.setContainerProperties(containerProperties);
            model.addJob(job);
            applicationIds.add(jobService.submitJob(job));
        }
        
        modelEntityMgr.post(model);
        modelEntityMgr.save();
        
        return applicationIds;
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
