package com.latticeengines.dataplatform.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.fs.PrototypeLocalResourcesFactoryBean.CopyEntry;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.dataplatform.util.HdfsHelper;

public class JobServiceImplTestNG extends DataPlatformFunctionalTestNGBase {

	@Autowired
	private JobService jobService;
	
	@BeforeClass(groups="functional")
	public void setup() throws Exception {
		FileSystem fs = FileSystem.get(yarnConfiguration);

		fs.delete(new Path("/training"), true);
		fs.delete(new Path("/test"), true);
		fs.delete(new Path("/datascientist1"), true);

		fs.mkdirs(new Path("/training"));
		fs.mkdirs(new Path("/test"));
		fs.mkdirs(new Path("/datascientist1"));

		List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();
		URL trainingFileUrl = ClassLoader.getSystemResource("com/latticeengines/dataplatform/service/impl/nn_train.dat");
		URL testFileUrl = ClassLoader.getSystemResource("com/latticeengines/dataplatform/service/impl/nn_test.dat");
		URL jsonUrl = ClassLoader.getSystemResource("com/latticeengines/dataplatform/service/impl/iris.json");
		URL pythonScriptUrl = ClassLoader.getSystemResource("com/latticeengines/dataplatform/service/impl/nn_train.py");
		URL modelUrl = ClassLoader.getSystemResource("com/latticeengines/dataplatform/service/impl/model.txt");
		String trainingFilePath = "file:" + trainingFileUrl.getFile();
		String testFilePath = "file:" + testFileUrl.getFile();
		String jsonFilePath = "file:" + jsonUrl.getFile();
		String pythonScriptPath = "file:" + pythonScriptUrl.getFile();
		FileUtils.copyFileToDirectory(new File(modelUrl.getFile()), new File("/tmp"));
		copyEntries.add(new CopyEntry(trainingFilePath, "/training", false));
		copyEntries.add(new CopyEntry(testFilePath, "/test", false));
		copyEntries.add(new CopyEntry(jsonFilePath, "/datascientist1", false));
		copyEntries.add(new CopyEntry(pythonScriptPath, "/datascientist1", false));
		doCopy(fs, copyEntries);
	}

	@Test(groups="functional", enabled=true)
	public void testGetJobReportsAll() throws Exception {
		List<ApplicationReport> applications = jobService.getJobReportsAll();
		assertNotNull(applications);
	}
	
	@Test(groups="functional", enabled=true)
	public void testKillApplication() throws Exception {
		Properties containerProperties = new Properties();
		containerProperties.put("VIRTUALCORES", "1");
		containerProperties.put("MEMORY", "64");
		containerProperties.put("PRIORITY", "0");
		ApplicationId applicationId = jobService.submitYarnJob("defaultYarnClient", containerProperties);
		YarnApplicationState state = waitState(applicationId, 30, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
		assertNotNull(state);
		jobService.killJob(applicationId);
		state = getState(applicationId);
		assertNotNull(state);
		assertTrue(state.equals(YarnApplicationState.KILLED));
	}

	@Test(groups="functional", enabled=true)
	public void testGetJobReportByUser() throws Exception {
		Properties containerProperties = new Properties();
		containerProperties.put("VIRTUALCORES", "1");
		containerProperties.put("MEMORY", "64");
		containerProperties.put("PRIORITY", "0");
		ApplicationId applicationId = jobService.submitYarnJob("defaultYarnClient", containerProperties);
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
		
		applicationId = jobService.submitYarnJob("defaultYarnClient", containerProperties);
		
		state = waitState(applicationId, 10, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
		reports = jobService.getJobReportByUser(app.getUser());
		assertTrue(reports.size() > numJobs);
		jobService.killJob(applicationId);
		
	}

	@Test(groups="functional")
	public void testSubmitPythonYarnJob() throws Exception {
		Properties containerProperties = new Properties();
		containerProperties.put("VIRTUALCORES", "1");
		containerProperties.put("MEMORY", "64");
		containerProperties.put("PRIORITY", "0");
		containerProperties.put("SCHEMA", "/datascientist1/iris.json");
		
		ApplicationId applicationId = jobService.submitYarnJob("pythonClient", containerProperties);
		YarnApplicationState state = waitState(applicationId, 30, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
		assertNotNull(state);
		ApplicationReport app = jobService.getJobReportById(applicationId);
		assertNotNull(app);
		state = waitState(applicationId, 120, TimeUnit.SECONDS, YarnApplicationState.FINISHED);
		assertEquals(state, YarnApplicationState.FINISHED);
		String modelContents = HdfsHelper.getHdfsFileContents(yarnConfiguration, "/datascientist1/result/model.txt");
		assertEquals(modelContents.trim(), "this is the generated model.");
	}

	/*
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
*/	
}
