package com.latticeengines.dataplatform.exposed.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.domain.Algorithm;
import com.latticeengines.dataplatform.exposed.domain.Classifier;
import com.latticeengines.dataplatform.exposed.domain.Model;
import com.latticeengines.dataplatform.exposed.domain.ModelDefinition;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.service.JobService;

@Component("modelingService")
public class ModelingServiceImpl implements ModelingService {

    @Autowired
    private JobService jobService;

    @Override
    public List<ApplicationId> submitModel(Model model) {
        List<ApplicationId> applicationIds = new ArrayList<ApplicationId>();
        ModelDefinition modelDefinition = model.getModelDefinition();

        List<Algorithm> algorithms = modelDefinition.getAlgorithms();

        for (Algorithm algorithm : algorithms) {
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

            applicationIds.add(jobService.submitYarnJob("pythonClient",
                    appMasterProperties, containerProperties));
        }
        return applicationIds;
    }

}
