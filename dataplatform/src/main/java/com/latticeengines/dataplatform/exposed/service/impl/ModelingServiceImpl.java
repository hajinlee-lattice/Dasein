package com.latticeengines.dataplatform.exposed.service.impl;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.domain.Model;
import com.latticeengines.dataplatform.exposed.service.ModelingService;

@Component("modelingService")
public class ModelingServiceImpl implements ModelingService {

	@Override
	public ApplicationId submitModel(Model model) {
		return null;
	}

}
