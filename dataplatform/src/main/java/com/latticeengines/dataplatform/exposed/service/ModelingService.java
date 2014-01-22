package com.latticeengines.dataplatform.exposed.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.dataplatform.exposed.domain.Model;

public interface ModelingService {

	ApplicationId submitModel(Model model);
}
