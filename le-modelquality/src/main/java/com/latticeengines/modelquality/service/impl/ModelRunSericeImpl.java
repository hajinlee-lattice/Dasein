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
public class ModelRunSericeImpl implements ModelRunService {

    private static final Log log = LogFactory.getLog(ModelRunSericeImpl.class);
    
    @Resource(name = "fileModelRunService")
    private ModelRunService fileModelRunService;
    
    @Resource(name = "eventTableModelRunService")
    private ModelRunService eventTableModelRunService;
    
    @Override
    public void run(ModelRun modelRun) {
        
        SelectedConfig config = modelRun.getSelectedConfig();
        DataSetType dataSetType = config.getDataSet().getDataSetType();
        if (dataSetType == DataSetType.SOURCETABLE) {
            fileModelRunService.run(modelRun);
        } else {
            eventTableModelRunService.run(modelRun);
        }
    }
}
