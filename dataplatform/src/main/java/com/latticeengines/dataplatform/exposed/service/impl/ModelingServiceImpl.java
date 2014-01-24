package com.latticeengines.dataplatform.exposed.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.domain.Model;
import com.latticeengines.dataplatform.exposed.domain.ModelDefinition;
import com.latticeengines.dataplatform.exposed.service.ModelingService;

@Component("modelingService")
public class ModelingServiceImpl implements ModelingService {

	@Override
	public ApplicationId submitModel(Model model) {
		return null;
	}

	@Override
	public List<ModelDefinition> getRegisteredModelDefinitions() {
		List<ModelDefinition> modelDefinitions = new ArrayList<ModelDefinition>();
		return modelDefinitions;
	}

}
