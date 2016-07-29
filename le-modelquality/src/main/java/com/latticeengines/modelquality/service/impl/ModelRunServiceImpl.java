package com.latticeengines.modelquality.service.impl;

import javax.annotation.Resource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.modelquality.DataSetType;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;
import com.latticeengines.modelquality.service.ModelRunService;

@Component("modelRunService")
public class ModelRunServiceImpl implements ModelRunService {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(ModelRunServiceImpl.class);
    
    @Resource(name = "fileModelRunService")
    private ModelRunService fileModelRunService;
    
    @Resource(name = "eventTableModelRunService")
    private ModelRunService eventTableModelRunService;
    
    @Override
    public String run(ModelRun modelRun) {
        SelectedConfig config = modelRun.getSelectedConfig();
        DataSetType dataSetType = config.getDataSet().getDataSetType();
        if (dataSetType == DataSetType.SOURCETABLE) {
            return fileModelRunService.run(modelRun);
        } else {
            return eventTableModelRunService.run(modelRun);
        }
    }
}
