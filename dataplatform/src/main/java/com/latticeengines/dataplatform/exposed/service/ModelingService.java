package com.latticeengines.dataplatform.exposed.service;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.dataplatform.exposed.domain.Model;

public interface ModelingService {

	List<ApplicationId> submitModel(Model model);
	
}
