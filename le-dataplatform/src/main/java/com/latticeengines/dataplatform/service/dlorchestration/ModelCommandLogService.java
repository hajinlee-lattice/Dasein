package com.latticeengines.dataplatform.service.dlorchestration;

import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;


public interface ModelCommandLogService {

    void log(ModelCommand modelCommand, String message);

    void logCompleteStep(ModelCommand modelCommand, ModelCommandStep step, ModelCommandStatus status);

    void logBeginStep(ModelCommand modelCommand, ModelCommandStep step);

    void logLedpException(ModelCommand modelCommand, LedpException e);

    void logException(ModelCommand modelCommand, Exception e);
}