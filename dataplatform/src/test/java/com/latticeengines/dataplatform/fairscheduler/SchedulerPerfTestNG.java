package com.latticeengines.dataplatform.fairscheduler;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.yarn.fs.PrototypeLocalResourcesFactoryBean.CopyEntry;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.domain.Classifier;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.functionalframework.SecureFileTransferAgent;
import com.latticeengines.dataplatform.service.JobService;

/**
<?xml version="1.0"?>
<allocations>
    <queue name="Priority0">
        <weight>1000</weight>
        <queue name="A">
            <minResources>1100 mb,2 vcores</minResources>
            <schedulingPolicy>fifo</schedulingPolicy>
        </queue>
        <queue name="B">
            <minResources>1100 mb,2 vcores</minResources>
            <schedulingPolicy>fifo</schedulingPolicy>
        </queue>
        <queue name="C">
            <minResources>1100 mb,2 vcores</minResources>
            <schedulingPolicy>fifo</schedulingPolicy>
        </queue>
    </queue>
    <queue name="Priority1">
        <weight>10</weight>
        <queue name="A">
            <schedulingPolicy>fifo</schedulingPolicy>
        </queue>
        <queue name="B">
            <schedulingPolicy>fifo</schedulingPolicy>
        </queue>
        <queue name="C">
            <schedulingPolicy>fifo</schedulingPolicy>
        </queue>

    </queue>
    <queue name="Priority2">
        <weight>1</weight>
        <queue name="A">
            <schedulingPolicy>fifo</schedulingPolicy>
        </queue>
        <queue name="B">
            <schedulingPolicy>fifo</schedulingPolicy>
        </queue>
        <queue name="C">
            <schedulingPolicy>fifo</schedulingPolicy>
        </queue>

    </queue>
    <defaultMinSharePreemptionTimeout>30</defaultMinSharePreemptionTimeout>
    <fairSharePreemptionTimeout>30</fairSharePreemptionTimeout>
</allocations>	 
*/
public class SchedulerPerfTestNG extends DataPlatformFunctionalTestNGBase {
	
	@Autowired
	private JobService jobService;
	
	@Autowired
	private SecureFileTransferAgent secureFileTransferAgent;

	@Value("${dataplatform.yarn.resourcemanager.fairscheduler.xml.location}")
	private String remoteFairSchedulerFilePath;
	
	private Classifier classifier1Min;
	private Classifier classifier2Mins;
	private Classifier classifier4Mins;

	protected boolean doYarnClusterSetup() {
		return false;
	}

	@BeforeClass(groups = "perf")
	public void setup() throws Exception {
		classifier1Min = new Classifier();
		classifier1Min.setName("IrisClassifier");
		classifier1Min.setFeatures(Arrays.<String>asList(new String[] { "sepal_length", "sepal_width", "petal_length", "petal_width"}));
		classifier1Min.setTargets(Arrays.<String>asList(new String[] { "category" }));
		classifier1Min.setSchemaHdfsPath("/scheduler/iris.json");
		classifier1Min.setModelHdfsDir("/scheduler/result");
		classifier1Min.setPythonScriptHdfsPath("/scheduler/train_1min.py");
		classifier1Min.setTrainingDataHdfsPath("/training/train.dat");
		classifier1Min.setTestDataHdfsPath("/test/test.dat");
		
		classifier2Mins = new Classifier();
		classifier2Mins.setName("IrisClassifier");
		classifier2Mins.setFeatures(Arrays.<String>asList(new String[] { "sepal_length", "sepal_width", "petal_length", "petal_width"}));
		classifier2Mins.setTargets(Arrays.<String>asList(new String[] { "category" }));
		classifier2Mins.setSchemaHdfsPath("/scheduler/iris.json");
		classifier2Mins.setModelHdfsDir("/scheduler/result");
		classifier2Mins.setPythonScriptHdfsPath("/scheduler/train_2mins.py");
		classifier2Mins.setTrainingDataHdfsPath("/training/train.dat");
		classifier2Mins.setTestDataHdfsPath("/test/test.dat");

		classifier4Mins = new Classifier();
		classifier4Mins.setName("IrisClassifier");
		classifier4Mins.setFeatures(Arrays.<String>asList(new String[] { "sepal_length", "sepal_width", "petal_length", "petal_width"}));
		classifier4Mins.setTargets(Arrays.<String>asList(new String[] { "category" }));
		classifier4Mins.setSchemaHdfsPath("/scheduler/iris.json");
		classifier4Mins.setModelHdfsDir("/scheduler/result");
		classifier4Mins.setPythonScriptHdfsPath("/scheduler/train_4mins.py");
		classifier4Mins.setTrainingDataHdfsPath("/training/train.dat");
		classifier4Mins.setTestDataHdfsPath("/test/test.dat");
		
		FileSystem fs = FileSystem.get(yarnConfiguration);

		fs.delete(new Path("/training"), true);
		fs.delete(new Path("/test"), true);
		fs.delete(new Path("/scheduler"), true);

		fs.mkdirs(new Path("/training"));
		fs.mkdirs(new Path("/test"));
		fs.mkdirs(new Path("/scheduler"));

		List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();
		URL trainingFileUrl = ClassLoader.getSystemResource("com/latticeengines/dataplatform/exposed/service/impl/train.dat");
		URL testFileUrl = ClassLoader.getSystemResource("com/latticeengines/dataplatform/exposed/service/impl/test.dat");
		URL jsonUrl = ClassLoader.getSystemResource("com/latticeengines/dataplatform/exposed/service/impl/iris.json");
		URL train1MinUrl = ClassLoader.getSystemResource("com/latticeengines/dataplatform/fairscheduler/train_1min.py");
		URL train2MinsUrl = ClassLoader.getSystemResource("com/latticeengines/dataplatform/fairscheduler/train_2mins.py");
		URL train4MinsUrl = ClassLoader.getSystemResource("com/latticeengines/dataplatform/fairscheduler/train_4mins.py");

		String trainingFilePath = "file:" + trainingFileUrl.getFile();
		String testFilePath = "file:" + testFileUrl.getFile();
		String jsonFilePath = "file:" + jsonUrl.getFile();
		String train1MinScriptPath = "file:" + train1MinUrl.getFile();
		String train2MinsScriptPath = "file:" + train2MinsUrl.getFile();
		String train4MinsScriptPath = "file:" + train4MinsUrl.getFile();

		copyEntries.add(new CopyEntry(trainingFilePath, "/training", false));
		copyEntries.add(new CopyEntry(testFilePath, "/test", false));
		copyEntries.add(new CopyEntry(jsonFilePath, "/scheduler", false));
		copyEntries.add(new CopyEntry(train1MinScriptPath, "/scheduler", false));
		copyEntries.add(new CopyEntry(train2MinsScriptPath, "/scheduler", false));
		copyEntries.add(new CopyEntry(train4MinsScriptPath, "/scheduler", false));

		doCopy(fs, copyEntries);
	}	
	
	@Test(groups = "perf", enabled = false)
	public void testSubmit() throws Exception {
		List<ApplicationId> appIds = new ArrayList<ApplicationId>();
		appIds.addAll(longRun("Priority0.A"));
		Thread.sleep(20000L);
		appIds.addAll(longRun("Priority0.B"));
	}
	
	@Test(groups = "perf", enabled = true)
	public void testSubmit2() throws Exception {
		List<ApplicationId> appIds = new ArrayList<ApplicationId>();
		// A
		for (int i = 0; i < 9; i++) {
			appIds.addAll(shortRun("A"));
			Thread.sleep(1000L);
		}
		Thread.sleep(7000L);

		// B
		appIds.addAll(shortRun("B"));
		Thread.sleep(5000L);
		
		// C
		appIds.addAll(shortRun("C"));
	}

	private Properties[] getPropertiesPair(Classifier classifier, String queue) {
		Properties containerProperties = new Properties();
		containerProperties.put("VIRTUALCORES", "1");
		containerProperties.put("MEMORY", "1024");
		containerProperties.put("PRIORITY", "0");
		containerProperties.put("METADATA", classifier.toString());
		
		Properties appMasterProperties = new Properties();
		appMasterProperties.put("QUEUE", queue);
		
		return new Properties[] { appMasterProperties, containerProperties };
	}
	
	private List<ApplicationId> shortRun(String queue) {
		List<ApplicationId> appIds = new ArrayList<ApplicationId>();
		Properties[] p0 = getPropertiesPair(classifier1Min, "Priority0." + queue);
		appIds.add(jobService.submitYarnJob("pythonClient", p0[0], p0[1]));
		
		for (int j = 0; j < 2; j++) {
			Properties[] p1 = getPropertiesPair(classifier2Mins, "Priority1." + queue);
			appIds.add(jobService.submitYarnJob("pythonClient", p1[0], p1[1]));
		}
		return appIds; 
	}

	private List<ApplicationId> longRun(String queue) {
		List<ApplicationId> appIds = new ArrayList<ApplicationId>();
		Properties[] p0 = getPropertiesPair(classifier1Min, "Priority0." + queue);
		appIds.add(jobService.submitYarnJob("pythonClient", p0[0], p0[1]));
		
		for (int j = 0; j < 2; j++) {
			Properties[] p1 = getPropertiesPair(classifier2Mins, "Priority1." + queue);
			appIds.add(jobService.submitYarnJob("pythonClient", p1[0], p1[1]));
		}

		for (int j = 0; j < 8; j++) {
			Properties[] p1 = getPropertiesPair(classifier4Mins, "Priority2." + queue);
			appIds.add(jobService.submitYarnJob("pythonClient", p1[0], p1[1]));
		}
		return appIds; 
	}
}
