package com.latticeengines.dataplatform.service.impl.dlorchestration;

import java.util.Date;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandResultEntityMgr;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepProcessor;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandResult;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus;

@Component("modelStepFinishProcessor")
public class ModelStepFinishProcessorImpl implements ModelStepProcessor {

    @Inject
    private ModelCommandEntityMgr modelCommandEntityMgr;

    @Inject
    private ModelCommandResultEntityMgr modelCommandResultEntityMgr;
    
    @Override
    public void executeStep(ModelCommand modelCommand, ModelCommandParameters modelCommandParameters) {
        ModelCommandResult result = modelCommandResultEntityMgr.findByModelCommand(modelCommand);
        result.setEndTime(new Date());
        result.setProcessStatus(ModelCommandStatus.SUCCESS);
        modelCommandResultEntityMgr.update(result);
        
        modelCommand.setCommandStatus(ModelCommandStatus.SUCCESS);
        modelCommandEntityMgr.update(modelCommand);
    }
}
