package com.latticeengines.dataplatform.fairscheduler;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.functionalframework.SecureFileTransferAgent;
import com.latticeengines.dataplatform.functionalframework.SecureFileTransferAgent.FileTransferOption;
import com.latticeengines.dataplatform.service.JobService;

@ContextConfiguration(locations = { "classpath:com/latticeengines/dataplatform/fairscheduler/FairSchedulerTestNG-context.xml" })
public class FairSchedulerTestNG extends DataPlatformFunctionalTestNGBase {

	@Autowired
	private JobService jobService;

	@Autowired
	private YarnService yarnService;

	@Autowired
	private SecureFileTransferAgent secureFileTransferAgent;

	@Value("${dataplatform.yarn.resourcemanager.fairscheduler.xml.location}")
	private String remoteFairSchedulerFilePath;

	@Value("${dataplatform.yarn.resourcemanager.log.location}")
	private String remoteRMLogPath;

	@BeforeMethod(groups = "functional")
	public void setup() throws Exception {

		FileSystem fileSystem = null;
		FSDataOutputStream fileOut = null;
		try {
			fileSystem = FileSystem.get(yarnConfiguration);
			String dir = "/output";
			Path path = new Path(dir);
			if (fileSystem.exists(path)) {
				fileSystem.delete(path, true);
				System.out.println("Deleted dir " + dir);
			}
			Path path1 = new Path("/output1");
			if (fileSystem.exists(path1)) {
				fileSystem.delete(path1, true);
				System.out.println("Deleted dir /output1");
			}
			Path path2 = new Path("/output2");
			if (fileSystem.exists(path2)) {
				fileSystem.delete(path2, true);
				System.out.println("Deleted dir /output2");
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
			throw new IllegalStateException(e); 
		} finally {
			try {
				fileOut.close();
				fileSystem.close();
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}

	}

	@Test(groups = "functional")
	public void testFairSchedulerJobVIP() throws Exception {

		File tempFairSchedulerFile = File.createTempFile("fair-scheduler", ".xml");

		// Fair Scheduler won't remove existing queue after refresh even if that
		// queue is removed from fair-scheduler.xml
		PrintWriter out = new PrintWriter(new FileWriter(tempFairSchedulerFile));
		out.println("<?xml version=\"1.0\"?>");
		out.println("<allocations>");
		out.println("	<queue name=\"Dell\">");
		out.println("		<weight>5</weight>");
		out.println("		<minResources>2048 mb,2 vcores</minResources>");
		out.println("		<schedulingPolicy>fair</schedulingPolicy>");
		out.println("		<minSharePreemptionTimeout>3</minSharePreemptionTimeout>");
		out.println("		<queue name=\"FastLane\">");
		out.println("			<weight>5</weight>");
		out.println("			<minResources>4096 mb,4 vcores</minResources>");
		out.println("			<fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
		out.println("			<schedulingPolicy>fifo</schedulingPolicy>");
		out.println("		</queue>");
		out.println("	</queue>");
		out.println("	<queue name=\"HP\">");
		out.println("		<weight>5</weight>");
		out.println("		<schedulingPolicy>fair</schedulingPolicy>");
		out.println("		<minResources>2048 mb,2 vcores</minResources>");
		out.println("		<queue name=\"FastLane\">");
		out.println("			<weight>5</weight>");
		out.println("			<minResources>4096 mb,4 vcores</minResources>");
		out.println("			<fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
		out.println("			<schedulingPolicy>fifo</schedulingPolicy>");
		out.println("		</queue>");
		out.println("	</queue>");
		out.println("	<queue name=\"VIP\">");
		out.println("		<weight>20</weight>");
		out.println("		<minResources>6096 mb,6 vcores</minResources>");
		out.println("		<schedulingPolicy>fair</schedulingPolicy>");
		out.println("		<queue name=\"FastLane\">");
		out.println("			<weight>20</weight>");
		out.println("			<minResources>8096 mb,8 vcores</minResources>");
		out.println("			<fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
		out.println("			<schedulingPolicy>fifo</schedulingPolicy>");
		out.println("		</queue>");
		out.println("	</queue>");
		out.println("	<defaultMinSharePreemptionTimeout>3</defaultMinSharePreemptionTimeout>");
		out.println("	<fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
		out.println("</allocations>");
		out.close();

		assertTrue(secureFileTransferAgent.fileTranser(tempFairSchedulerFile.getAbsolutePath(),
				remoteFairSchedulerFilePath, FileTransferOption.UPLOAD));

		Thread.sleep(15000L);
		Properties configuration = new Properties();
		configuration.put("mapreduce.job.queuename", "Dell.FastLane");
		ApplicationId applicationIdDell = jobService.submitMRJob("wordCountJob1", configuration);
		configuration.put("mapreduce.job.queuename", "HP.FastLane");
		ApplicationId applicationIdHP = jobService.submitMRJob("wordCountJob2", configuration);
		configuration.put("mapreduce.job.queuename", "VIP.FastLane");
		ApplicationId applicationIdVIP = jobService.submitMRJob("wordCountJob", configuration);
		YarnApplicationState state = waitState(applicationIdVIP, 300, TimeUnit.SECONDS, YarnApplicationState.FAILED,
				YarnApplicationState.FINISHED);

		state = getState(applicationIdVIP);
		assertNotNull(state);
		assertTrue(state.equals(YarnApplicationState.FINISHED));
		long vipJobFinishTime = jobService.getJobReportById(applicationIdVIP).getFinishTime();

		state = getState(applicationIdDell);
		assertNotNull(state);
		if (state.equals(YarnApplicationState.FINISHED)) {
			long dellJobFinishTime = jobService.getJobReportById(applicationIdDell).getFinishTime();
			assertTrue(dellJobFinishTime >= vipJobFinishTime);
		} else {
			jobService.killJob(applicationIdDell);
		}

		state = getState(applicationIdHP);
		if (state.equals(YarnApplicationState.FINISHED)) {
			long hpJobFinishTime = jobService.getJobReportById(applicationIdHP).getFinishTime();
			assertTrue(hpJobFinishTime >= vipJobFinishTime);
		} else {
			jobService.killJob(applicationIdHP);
		}

	}

	@Test(groups = "functional")
	public void testFairSchedulerJobFailure() throws Exception {

		File tempFairSchedulerFile = File.createTempFile("fair-scheduler", ".xml");

		// Fair Scheduler won't remove existing queue after refresh even if that
		// queue is removed from fair-scheduler.xml
		PrintWriter out = new PrintWriter(new FileWriter(tempFairSchedulerFile));
		out.println("<?xml version=\"1.0\"?>");
		out.println("<allocations>");
		out.println("	<queue name=\"Dell\">");
		out.println("		<weight>5</weight>");
		out.println("		<minResources>4096 mb,4 vcores</minResources>");
		out.println("		<schedulingPolicy>fair</schedulingPolicy>");
		out.println("		<minSharePreemptionTimeout>3</minSharePreemptionTimeout>");
		out.println("		<queue name=\"FastLane\">");
		out.println("			<weight>5</weight>");
		out.println("			<minResources>4096 mb,4 vcores</minResources>");
		out.println("			<fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
		out.println("			<schedulingPolicy>fifo</schedulingPolicy>");
		out.println("		</queue>");
		out.println("	</queue>");
		out.println("	<queue name=\"HP\">");
		out.println("		<weight>5</weight>");
		out.println("		<schedulingPolicy>fair</schedulingPolicy>");
		out.println("		<minResources>4096 mb,4 vcores</minResources>");
		out.println("		<queue name=\"FastLane\">");
		out.println("			<weight>5</weight>");
		out.println("			<minResources>4096 mb,4 vcores</minResources>");
		out.println("			<fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
		out.println("			<schedulingPolicy>fifo</schedulingPolicy>");
		out.println("		</queue>");
		out.println("	</queue>");
		out.println("	<queue name=\"Common\">");
		out.println("		<weight>2</weight>");
		out.println("		<schedulingPolicy>fair</schedulingPolicy>");
		out.println("		<queue name=\"FastLane\">");
		out.println("			<weight>2</weight>");
		out.println("			<fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
		out.println("			<schedulingPolicy>fifo</schedulingPolicy>");
		out.println("		</queue>");
		out.println("	</queue>");
		out.println("	<queue name=\"VIP\">");
		out.println("		<weight>20</weight>");
		out.println("		<minResources>6096 mb,6 vcores</minResources>");
		out.println("		<schedulingPolicy>fair</schedulingPolicy>");
		out.println("		<queue name=\"FastLane\">");
		out.println("			<weight>5</weight>");
		out.println("			<minResources>8096 mb,8 vcores</minResources>");
		out.println("			<fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
		out.println("			<schedulingPolicy>fifo</schedulingPolicy>");
		out.println("		</queue>");
		out.println("	</queue>");
		out.println("	<queue name=\"Experimental\">");
		out.println("		<schedulingPolicy>fifo</schedulingPolicy>");
		out.println("	</queue>");
		out.println("	<defaultMinSharePreemptionTimeout>3</defaultMinSharePreemptionTimeout>");
		out.println("	<fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
		out.println("</allocations>");
		out.close();

		secureFileTransferAgent.fileTranser(tempFairSchedulerFile.getAbsolutePath(), remoteFairSchedulerFilePath,
				FileTransferOption.UPLOAD);

		Thread.sleep(15000L);
		Properties configuration = new Properties();
		configuration.put("mapreduce.job.queuename", "Common.FastLane");
		ApplicationId applicationId = jobService.submitMRJob("wordCountJob", configuration);
		configuration.put("mapreduce.job.queuename", "Dell.FastLane");
		ApplicationId applicationId2 = jobService.submitMRJob("wordCountJob1", configuration);
		configuration.put("mapreduce.job.queuename", "HP.FastLane");
		ApplicationId applicationId3 = jobService.submitMRJob("wordCountJob2", configuration);
		YarnApplicationState state = waitState(applicationId2, 120, TimeUnit.SECONDS, YarnApplicationState.FAILED,
				YarnApplicationState.FINISHED);
		assertNotNull(state);
		assertTrue(state.equals(YarnApplicationState.FINISHED));

		state = getState(applicationId3);
		if (!state.equals(YarnApplicationState.FINISHED)) {
			state = waitState(applicationId3, 120, TimeUnit.SECONDS, YarnApplicationState.FINISHED);
		}
		assertNotNull(state);
		assertTrue(state.equals(YarnApplicationState.FINISHED));
		
		state = getState(applicationId);
		assertNotNull(state);
		assertTrue(state.equals(YarnApplicationState.FAILED)); // this job fails after two preemption

		verifyPreemption(applicationId);
	}

	@Test(groups = "functional")
	public void testFairSchedulerJob() throws Exception {

		File tempFairSchedulerFile = File.createTempFile("fair-scheduler", ".xml");

		// Fair Scheduler won't remove existing queue after refresh even if that
		// queue is removed from fair-scheduler.xml
		PrintWriter out = new PrintWriter(new FileWriter(tempFairSchedulerFile));
		out.println("<?xml version=\"1.0\"?>");
		out.println("<allocations>");
		out.println("	<queue name=\"Dell\">");
		out.println("		<weight>5</weight>");
		out.println("		<minResources>4096 mb,4 vcores</minResources>");
		out.println("		<schedulingPolicy>fair</schedulingPolicy>");
		out.println("		<minSharePreemptionTimeout>3</minSharePreemptionTimeout>");
		out.println("		<queue name=\"FastLane\">");
		out.println("			<weight>5</weight>");
		out.println("			<minResources>4096 mb,4 vcores</minResources>");
		out.println("			<fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
		out.println("			<schedulingPolicy>fifo</schedulingPolicy>");
		out.println("		</queue>");
		out.println("	</queue>");
		out.println("	<queue name=\"HP\">");
		out.println("		<weight>5</weight>");
		out.println("		<schedulingPolicy>fair</schedulingPolicy>");
		out.println("		<minResources>4096 mb,4 vcores</minResources>");
		out.println("		<queue name=\"FastLane\">");
		out.println("			<weight>5</weight>");
		out.println("			<minResources>4096 mb,4 vcores</minResources>");
		out.println("			<fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
		out.println("			<schedulingPolicy>fifo</schedulingPolicy>");
		out.println("		</queue>");
		out.println("	</queue>");
		out.println("	<queue name=\"Common\">");
		out.println("		<weight>2</weight>");
		out.println("		<schedulingPolicy>fair</schedulingPolicy>");
		out.println("		<queue name=\"FastLane\">");
		out.println("			<weight>2</weight>");
		out.println("			<minResources>2048 mb,2 vcores</minResources>");
		out.println("			<fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
		out.println("			<schedulingPolicy>fifo</schedulingPolicy>");
		out.println("		</queue>");
		out.println("	</queue>");
		out.println("	<queue name=\"VIP\">");
		out.println("		<weight>20</weight>");
		out.println("		<minResources>6096 mb,6 vcores</minResources>");
		out.println("		<schedulingPolicy>fair</schedulingPolicy>");
		out.println("		<queue name=\"FastLane\">");
		out.println("			<weight>5</weight>");
		out.println("			<minResources>8096 mb,8 vcores</minResources>");
		out.println("			<fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
		out.println("			<schedulingPolicy>fifo</schedulingPolicy>");
		out.println("		</queue>");
		out.println("	</queue>");
		out.println("	<queue name=\"Experimental\">");
		out.println("		<schedulingPolicy>fifo</schedulingPolicy>");
		out.println("	</queue>");
		out.println("	<defaultMinSharePreemptionTimeout>3</defaultMinSharePreemptionTimeout>");
		out.println("	<fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
		out.println("</allocations>");
		out.close();

		assertTrue(secureFileTransferAgent.fileTranser(tempFairSchedulerFile.getAbsolutePath(),
				remoteFairSchedulerFilePath, FileTransferOption.UPLOAD));

		Properties configuration = new Properties();
		configuration.put("mapreduce.job.queuename", "Common.FastLane");
		ApplicationId applicationId = jobService.submitMRJob("wordCountJob", configuration);
		configuration.put("mapreduce.job.queuename", "Dell.FastLane");
		ApplicationId applicationId2 = jobService.submitMRJob("wordCountJob1", configuration);
		configuration.put("mapreduce.job.queuename", "HP.FastLane");
		ApplicationId applicationId3 = jobService.submitMRJob("wordCountJob2", configuration);
		YarnApplicationState state = waitState(applicationId, 300, TimeUnit.SECONDS, YarnApplicationState.FAILED,
				YarnApplicationState.FINISHED);

		assertNotNull(state);
		assertTrue(state.equals(YarnApplicationState.FINISHED));
		long commonJobFinishTime = jobService.getJobReportById(applicationId).getFinishTime();
		long hpJobFinishTime = jobService.getJobReportById(applicationId3).getFinishTime();
		long dellJobFinishTime = jobService.getJobReportById(applicationId2).getFinishTime();
		System.out.println("HPJobFinishTime " + hpJobFinishTime);
		System.out.println("CommonJobFinishTime " + commonJobFinishTime);
		System.out.println("DellJobFinishTime " + dellJobFinishTime);
		assertTrue(hpJobFinishTime <= commonJobFinishTime);
		assertTrue(dellJobFinishTime <= commonJobFinishTime);

	}

	@Test(groups = "functional")
	public void testFairSchedulerPreemptingJob() throws Exception {

		File tempFairSchedulerFile = File.createTempFile("fair-scheduler", ".xml");

		// Fair Scheduler won't remove existing queue after refresh even if that
		// queue is removed from fair-scheduler.xml
		PrintWriter out = new PrintWriter(new FileWriter(tempFairSchedulerFile));
		out.println("<?xml version=\"1.0\"?>");
		out.println("<allocations>");
		out.println("	<queue name=\"Dell\">");
		out.println("		<weight>5</weight>");
		out.println("		<schedulingPolicy>fair</schedulingPolicy>");
		out.println("		<minResources>4096 mb,4 vcores</minResources>");
		out.println("		<queue name=\"FastLane\">");
		out.println("			<weight>5</weight>");
		out.println("			<minResources>4096 mb,4 vcores</minResources>");
		out.println("			<fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
		out.println("			<schedulingPolicy>fifo</schedulingPolicy>");
		out.println("		</queue>");
		out.println("	</queue>");
		out.println("	<queue name=\"Common\">");
		out.println("		<weight>2</weight>");
		out.println("		<schedulingPolicy>fair</schedulingPolicy>");
		out.println("		<queue name=\"FastLane\">");
		out.println("			<weight>5</weight>");
		out.println("			<fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
		out.println("			<schedulingPolicy>fifo</schedulingPolicy>");
		out.println("		</queue>");
		out.println("	</queue>");
		out.println("	<queue name=\"VIP\">");
		out.println("		<weight>20</weight>");
		out.println("		<minResources>6096 mb,6 vcores</minResources>");
		out.println("		<schedulingPolicy>fair</schedulingPolicy>");
		out.println("		<queue name=\"FastLane\">");
		out.println("			<weight>5</weight>");
		out.println("			<minResources>8096 mb,8 vcores</minResources>");
		out.println("			<fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
		out.println("			<schedulingPolicy>fifo</schedulingPolicy>");
		out.println("		</queue>");
		out.println("	</queue>");
		out.println("	<defaultMinSharePreemptionTimeout>3</defaultMinSharePreemptionTimeout>");
		out.println("	<fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>");
		out.println("</allocations>");
		out.close();

		assertTrue(secureFileTransferAgent.fileTranser(tempFairSchedulerFile.getAbsolutePath(),
				remoteFairSchedulerFilePath, FileTransferOption.UPLOAD));

		// Sleep for 20s to allow fair scheduler to pickup new setting
		Thread.sleep(20000);

		// SchedulerInfo is private field from SchedulerTypeInfo
		SchedulerTypeInfo schedulerInfo = yarnService.getSchedulerInfo();
		Field field = SchedulerTypeInfo.class.getDeclaredField("schedulerInfo");
		field.setAccessible(true);
		FairSchedulerInfo fairScheduler = (FairSchedulerInfo) field.get(schedulerInfo);
		Iterator iter = fairScheduler.getRootQueueInfo().getChildQueues().iterator();
		while (iter.hasNext()) {
			FairSchedulerQueueInfo queue = (FairSchedulerQueueInfo) iter.next();
			if (queue.getQueueName().equalsIgnoreCase("root.Dell")) {
				assertTrue(queue.getSchedulingPolicy().equalsIgnoreCase("fair"));
				System.out.println(queue.getMinResources().toString());
				assertTrue(queue.getMinResources().getMemory() == 4096);
				assertTrue(queue.getMinResources().getvCores() == 4);
			}
		}

		Properties configuration = new Properties();
		configuration.put("mapreduce.job.queuename", "Common.FastLane");
		ApplicationId applicationId = jobService.submitMRJob("wordCountJob", configuration);
		Thread.sleep(10000);
		configuration.put("mapreduce.job.queuename", "Dell.FastLane");
		// configuration.put("mapreduce.map.memory.mb", "4096");
		ApplicationId applicationId2 = jobService.submitMRJob("wordCountJob1", configuration);
		YarnApplicationState state = waitState(applicationId, 600, TimeUnit.SECONDS, YarnApplicationState.FAILED,
				YarnApplicationState.FINISHED);

		state = getState(applicationId);
		assertNotNull(state);
		assertTrue(!state.equals(YarnApplicationState.FAILED));
		long commonJobFinishTime = jobService.getJobReportById(applicationId).getFinishTime();
		long dellJobFinishTime = jobService.getJobReportById(applicationId2).getFinishTime();
		System.out.println("CommonJobFinishTime " + commonJobFinishTime);
		System.out.println("DellJobFinishTime " + dellJobFinishTime);
		assertTrue(dellJobFinishTime <= commonJobFinishTime);
		
		verifyPreemption(applicationId);
	}

	private void verifyPreemption(ApplicationId applicationId) throws IOException, FileNotFoundException {
		File tempRMLogFile = File.createTempFile("resource-manager", ".log");

		assertTrue(secureFileTransferAgent.fileTranser(tempRMLogFile.getAbsolutePath(), remoteRMLogPath,
				FileTransferOption.DOWNLOAD));

		BufferedReader br = new BufferedReader(new FileReader(tempRMLogFile));
		boolean isPreemptContainer = false;
		try {
			String line = br.readLine();
			Pattern pattern = Pattern.compile("Preempting container.+?from queue root.Common.FastLane");
			Matcher matcher = null;

			boolean foundApplication = false;
			String applicationIdStr = applicationId.toString();
			while (line != null) {
				if (!foundApplication && line.contains(applicationIdStr)) {
					foundApplication = true;
				}
				if (foundApplication) {
					matcher = pattern.matcher(line);
					if (matcher.find()) {
						isPreemptContainer = true;
					}
				}
				line = br.readLine();
			}
		} finally {
			br.close();
		}
		assertTrue(isPreemptContainer);
	}

}
