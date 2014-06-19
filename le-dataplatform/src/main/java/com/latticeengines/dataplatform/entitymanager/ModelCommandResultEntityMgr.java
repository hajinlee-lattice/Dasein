package com.latticeengines.dataplatform.entitymanager;

import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandResult;

public interface ModelCommandResultEntityMgr extends BaseEntityMgr<ModelCommandResult> {

    ModelCommandResult findByModelCommand(ModelCommand modelCommand);
    
}
