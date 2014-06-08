package com.latticeengines.dataplatform.service;

import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;


public interface ModelCommandLogService {

    void log(int commandId, String message);

    void logCompleteStep(int commandId, ModelCommandStep step, ModelCommandStatus status);

    void logBeginStep(int commandId, ModelCommandStep step);

    void logLedpException(int commandId, LedpException e);

    void logException(int commandId, Exception e);
}