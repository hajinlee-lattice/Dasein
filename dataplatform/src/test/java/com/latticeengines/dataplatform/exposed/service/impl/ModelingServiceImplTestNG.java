package com.latticeengines.dataplatform.exposed.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

public class ModelingServiceImplTestNG extends DataPlatformFunctionalTestNGBase {

	@Autowired
	private ModelingService modelingService;
	
	@Test(groups = "functional")
	public void submitModel() {
		modelingService.submitModel(null);
	}
}
