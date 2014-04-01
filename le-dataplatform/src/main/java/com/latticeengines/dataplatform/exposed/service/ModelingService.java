package com.latticeengines.dataplatform.exposed.service;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.dataplatform.exposed.domain.Model;
import com.latticeengines.dataplatform.exposed.domain.SamplingConfiguration;
import com.latticeengines.dataplatform.exposed.domain.ThrottleConfiguration;

public interface ModelingService {

    List<ApplicationId> submitModel(Model model);

    void throttle(ThrottleConfiguration config);

	void setupCustomer(String customerName);
	
	ApplicationId createSamples(SamplingConfiguration config);
}
