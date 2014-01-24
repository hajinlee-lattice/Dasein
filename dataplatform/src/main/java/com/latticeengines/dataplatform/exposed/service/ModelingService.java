package com.latticeengines.dataplatform.exposed.service;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.dataplatform.exposed.domain.Model;
import com.latticeengines.dataplatform.exposed.domain.ModelDefinition;

public interface ModelingService {

	ApplicationId submitModel(Model model);
	
	List<ModelDefinition> getRegisteredModelDefinitions();
}
