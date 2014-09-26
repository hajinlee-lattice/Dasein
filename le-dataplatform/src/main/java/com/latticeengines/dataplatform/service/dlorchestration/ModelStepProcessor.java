package com.latticeengines.dataplatform.service.dlorchestration;

import org.apache.hadoop.conf.Configuration;

import com.latticeengines.dataplatform.service.impl.dlorchestration.ModelCommandParameters;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;

public interface ModelStepProcessor {

    void executeStep(ModelCommand modelCommand, ModelCommandParameters modelCommandParameters);

	Configuration getConfiguration();

	String getCustomerBaseDir();

}
