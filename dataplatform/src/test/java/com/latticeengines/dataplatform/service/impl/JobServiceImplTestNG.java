package com.latticeengines.dataplatform.service.impl;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
	
	@Test(groups="functional")
	public void testGetJobReportsAll() throws Exception {
		List<ApplicationReport> applications = jobService.getJobReportsAll();
		assertNotNull(applications);
	}

	@Test(groups="functional")
	public void testKillApplication() throws Exception {
		ApplicationId applicationId = submitApplication();
		YarnApplicationState state = waitState(applicationId, 30, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
		assertNotNull(state);
		jobService.killJob(applicationId);
		state = getState(applicationId);
		assertNotNull(state);
		assertTrue(state.equals(YarnApplicationState.KILLED));
	}

	@Test(groups="functional")
	public void testGetJobReportByUser() throws Exception {
		ApplicationId applicationId = submitApplication();
		YarnApplicationState state = waitState(applicationId, 30, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
		assertNotNull(state);
		jobService.killJob(applicationId);
		state = getState(applicationId);
		assertNotNull(state);
		assertTrue(state.equals(YarnApplicationState.KILLED));
		
		ApplicationReport app = jobService.getJobReportById(applicationId);
		
		List<ApplicationReport> reports = jobService.getJobReportByUser(app.getUser());
		int numJobs = reports.size();
		assertTrue(numJobs > 0);
		
		applicationId = submitApplication();
		
		state = waitState(applicationId, 10, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
		reports = jobService.getJobReportByUser(app.getUser());
		assertTrue(reports.size() > numJobs);
		jobService.killJob(applicationId);
		
	}

	@Test(groups="functional")
	public void testSubmitYarnJob() throws Exception {
		ApplicationId applicationId = jobService.submitYarnJob("anotherYarnClient");
		YarnApplicationState state = waitState(applicationId, 30, TimeUnit.SECONDS, YarnApplicationState.FINISHED);

		state = getState(applicationId);
		assertNotNull(state);
		assertTrue(!state.equals(YarnApplicationState.FAILED));
		
		ApplicationReport app = jobService.getJobReportById(applicationId);
		
		List<ApplicationReport> reports = jobService.getJobReportByUser(app.getUser());
		int numJobs = reports.size();
		assertTrue(numJobs > 0);
		jobService.killJob(applicationId);
	}

	@Test(groups="functional")
	public void testSubmitMRJob() throws Exception {
		
		Configuration conf = (Configuration) applicationContext.getBean("hadoopConfiguration");
		FileSystem fileSystem = null;
		FSDataOutputStream fileOut = null;
		try {
			fileSystem = FileSystem.get(conf);
			String dir = "/output";
		    Path path = new Path(dir);
		    if (fileSystem.exists(path)) {
		    	fileSystem.delete(path, true);
		        System.out.println("Deleted dir " + dir);
		    }
		    
		    Path inputFilepath = new Path("/input/file1.txt");
		    if (fileSystem.exists(inputFilepath)) {
		    	fileSystem.delete(inputFilepath, true);
		    }
		    
		    // Create a new file and write data to it.
		    fileOut = fileSystem.create(inputFilepath);
		    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileOut));
		    writer.write("Watson is awesome\n");
		    writer.flush();
		    fileOut.flush();	    
		} catch (IOException e) {
			e.printStackTrace();
		} finally {		
		    try {
				fileOut.close();
			    fileSystem.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		ApplicationId applicationId = jobService.submitMRJob("wordCountJob");
		YarnApplicationState state = waitState(applicationId, 120, TimeUnit.SECONDS, YarnApplicationState.FINISHED);

		state = getState(applicationId);
		assertNotNull(state);
		assertTrue(!state.equals(YarnApplicationState.FAILED));
	}
	
}
