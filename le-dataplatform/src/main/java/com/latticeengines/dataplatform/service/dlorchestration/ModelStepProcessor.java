package com.latticeengines.dataplatform.service.dlorchestration;

import com.latticeengines.dataplatform.service.impl.dlorchestration.ModelCommandParameters;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;

public interface ModelStepProcessor {

    void executePostStep(ModelCommand modelCommand, ModelCommandParameters modelCommandParameters);

}
