package com.latticeengines.dataplatform.exposed.service.impl;

import static org.testng.Assert.assertNotNull;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

public class YarnServiceImplTestNG extends DataPlatformFunctionalTestNGBase {

	@Autowired
	private YarnService yarnService;
	
	protected boolean doYarnClusterSetup() {
		return false;
	}
	
	@Test(groups="functional")
	public void getSchedulerInfo() {
		SchedulerTypeInfo schedulerInfo = yarnService.getSchedulerInfo();
		assertNotNull(schedulerInfo);
	}

	@Test(groups="functional")
	public void getApps() {
		AppsInfo appsInfo = yarnService.getApplications();
		assertNotNull(appsInfo);
	}
}
