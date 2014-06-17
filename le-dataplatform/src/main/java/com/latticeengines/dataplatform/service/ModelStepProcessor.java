package com.latticeengines.dataplatform.service;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandParameter;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;

public interface ModelStepProcessor {

    List<ApplicationId> executeYarnStep(String deploymentId, ModelCommandStep currentStep, List<ModelCommandParameter> commandParameters);

}
