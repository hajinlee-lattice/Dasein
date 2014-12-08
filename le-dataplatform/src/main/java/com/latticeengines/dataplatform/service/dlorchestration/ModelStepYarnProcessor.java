package com.latticeengines.dataplatform.service.dlorchestration;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.dataplatform.service.impl.dlorchestration.ModelCommandParameters;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;

public interface ModelStepYarnProcessor {

    List<ApplicationId> executeYarnStep(String deploymentId, ModelCommandStep currentStep, ModelCommandParameters commandParameters, ModelCommand modelCommand);

}
