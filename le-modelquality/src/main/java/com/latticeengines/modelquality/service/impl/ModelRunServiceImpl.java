package com.latticeengines.modelquality.service.impl;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.DataSetType;
import com.latticeengines.domain.exposed.modelquality.Environment;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.domain.exposed.modelquality.ModelRunEntityNames;
import com.latticeengines.modelquality.entitymgr.DataSetEntityMgr;
import com.latticeengines.modelquality.service.ModelRunService;

@Component("modelRunService")
public class ModelRunServiceImpl extends BaseServiceImpl implements ModelRunService {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ModelRunServiceImpl.class);

    @Resource(name = "fileModelRunService")
    private ModelRunService fileModelRunService;

    @Resource(name = "eventTableModelRunService")
    private ModelRunService eventTableModelRunService;

    @Autowired
    private DataSetEntityMgr dataSetEntityMgr;

    @Override
    public ModelRun createModelRun(ModelRunEntityNames modelRunEntityNames, Environment env) {
        String datasetName = modelRunEntityNames.getDataSetName();
        DataSet dataset = dataSetEntityMgr.findByName(datasetName);
        DataSetType dataSetType = dataset.getDataSetType();
        if (dataSetType == DataSetType.FILE) {
            return fileModelRunService.createModelRun(modelRunEntityNames, env);
        } else {
            return eventTableModelRunService.createModelRun(modelRunEntityNames, env);
        }
    }

    @Override
    public void setEnvironment(Environment env) {
        fileModelRunService.setEnvironment(env);
        fileModelRunService.setEnvironment(env);
    }
}
