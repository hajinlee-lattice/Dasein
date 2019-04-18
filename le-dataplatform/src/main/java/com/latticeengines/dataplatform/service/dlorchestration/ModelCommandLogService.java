package com.latticeengines.dataplatform.service.dlorchestration;

import java.util.List;

import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandLog;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;
import com.latticeengines.domain.exposed.exception.LedpException;


public interface ModelCommandLogService {

    void log(ModelCommand modelCommand, String message);

    void logCompleteStep(ModelCommand modelCommand, ModelCommandStep step, ModelCommandStatus status);

    void logYarnAppId(ModelCommand modelCommand, String yarnAppId, ModelCommandStep step);

    void logBeginStep(ModelCommand modelCommand, ModelCommandStep step);

    void logLedpException(ModelCommand modelCommand, LedpException e);

    void logException(ModelCommand modelCommand, Exception e);

    void logException(ModelCommand modelCommand, String message, Exception e);

    List<ModelCommandLog> findByModelCommand(ModelCommand modelCommand);
}
