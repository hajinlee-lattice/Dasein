package com.latticeengines.dataplatform.service.impl;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.JobService;

@ContextConfiguration(locations = {
    "classpath:com/latticeengines/dataplatform/service/impl/JobServiceImplTestNG-context.xml"
})
public class JobServiceImplTestNG extends DataPlatformFunctionalTestNGBase {

	@Autowired
	private JobService jobService;
	
	@Test
	public void testGetJobReportsAll() throws Exception {
		List<ApplicationReport> applications = jobService.getJobReportsAll();
		assertNotNull(applications);
	}

	@Test
	public void testKillApplication() throws Exception {
		ApplicationId applicationId = submitApplication();
		YarnApplicationState state = waitState(applicationId, 120, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
		assertNotNull(state);
		jobService.killJob(applicationId);
		state = getState(applicationId);
		assertNotNull(state);
		assertTrue(state.equals(YarnApplicationState.KILLED));
	}

	@Test
	public void testGetJobReportByUser() throws Exception {
		ApplicationId applicationId = submitApplication();
		YarnApplicationState state = waitState(applicationId, 120, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
		assertNotNull(state);
		jobService.killJob(applicationId);
		state = getState(applicationId);
		assertNotNull(state);
		assertTrue(state.equals(YarnApplicationState.KILLED));
		
		ApplicationReport app = jobService.getJobReportById(applicationId);
		
		List<ApplicationReport> reports = jobService.getJobReportByUser(app.getUser());
		int numJobs = reports.size();
		assertTrue(numJobs > 0);
		
		submitApplication();
		
		state = waitState(applicationId, 120, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
		reports = jobService.getJobReportByUser(app.getUser());
		assertTrue(reports.size() > numJobs);
	}

	@Test
	public void testSubmitJob() throws Exception {
		ApplicationId applicationId = jobService.submitJob("anotherYarnClient");
		YarnApplicationState state = waitState(applicationId, 120, TimeUnit.SECONDS, YarnApplicationState.RUNNING);

		state = getState(applicationId);
		assertNotNull(state);
		assertTrue(!state.equals(YarnApplicationState.KILLED));
		
		ApplicationReport app = jobService.getJobReportById(applicationId);
		
		List<ApplicationReport> reports = jobService.getJobReportByUser(app.getUser());
		int numJobs = reports.size();
		assertTrue(numJobs > 0);
	}

}
