package com.latticeengines.dataplatform.exposed.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.fs.PrototypeLocalResourcesFactoryBean.CopyEntry;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.domain.Algorithm;
import com.latticeengines.dataplatform.exposed.domain.Model;
import com.latticeengines.dataplatform.exposed.domain.ModelDefinition;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

public class ModelingServiceImplTestNG extends DataPlatformFunctionalTestNGBase {

	@Autowired
	private ModelingService modelingService;
	
	@BeforeClass(groups="functional")
	public void setup() throws Exception {
		FileSystem fs = FileSystem.get(yarnConfiguration);

		fs.delete(new Path("/training"), true);
		fs.delete(new Path("/test"), true);
		fs.delete(new Path("/datascientist2"), true);

		fs.mkdirs(new Path("/training"));
		fs.mkdirs(new Path("/test"));
		fs.mkdirs(new Path("/datascientist2"));

		List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();
		URL trainingFileUrl = ClassLoader.getSystemResource("com/latticeengines/dataplatform/exposed/service/impl/train.dat");
		URL testFileUrl = ClassLoader.getSystemResource("com/latticeengines/dataplatform/exposed/service/impl/test.dat");
		URL jsonUrl = ClassLoader.getSystemResource("com/latticeengines/dataplatform/exposed/service/impl/iris.json");
		URL neuralNetworkPythonScriptUrl = ClassLoader.getSystemResource("com/latticeengines/dataplatform/exposed/service/impl/nn_train.py");
		URL logisticRegressionPythonScriptUrl = ClassLoader.getSystemResource("com/latticeengines/dataplatform/exposed/service/impl/lr_train.py");

		String trainingFilePath = "file:" + trainingFileUrl.getFile();
		String testFilePath = "file:" + testFileUrl.getFile();
		String jsonFilePath = "file:" + jsonUrl.getFile();
		String neuralNetworkPythonScriptPath = "file:" + neuralNetworkPythonScriptUrl.getFile();
		String logisticRegressionPythonScriptPath = "file:" + logisticRegressionPythonScriptUrl.getFile();

		copyEntries.add(new CopyEntry(trainingFilePath, "/training", false));
		copyEntries.add(new CopyEntry(testFilePath, "/test", false));
		copyEntries.add(new CopyEntry(jsonFilePath, "/datascientist2", false));
		copyEntries.add(new CopyEntry(neuralNetworkPythonScriptPath, "/datascientist2", false));
		copyEntries.add(new CopyEntry(logisticRegressionPythonScriptPath, "/datascientist2", false));

		doCopy(fs, copyEntries);
	}

	@Test(groups="functional")
	public void submitModel() throws Exception {
		Algorithm neuralNetworkAlgorithm = new Algorithm();
		neuralNetworkAlgorithm.setName("NN");
		neuralNetworkAlgorithm.setScript("/datascientist2/nn_train.py");

		Algorithm logisticRegressionAlgorithm = new Algorithm();
		logisticRegressionAlgorithm.setName("LR");
		logisticRegressionAlgorithm.setScript("/datascientist2/lr_train.py");
		
		ModelDefinition modelDef = new ModelDefinition();
		modelDef.setName("Model1");
		modelDef.setAlgorithms(Arrays.<Algorithm>asList(
				new Algorithm[] { neuralNetworkAlgorithm, logisticRegressionAlgorithm }));
		
		Model model = new Model();
		model.setModelDefinition(modelDef);
		model.setName("Model Submission1");
		model.setSchemaHdfsPath("/datascientist2/iris.json");
		model.setTrainingDataHdfsPath("/training/train.dat");
		model.setTestDataHdfsPath("/test/test.dat");
		model.setModelHdfsDir("/datascientist2/model");
		
		System.out.println(model.toString());
		
		List<ApplicationId> appIds = modelingService.submitModel(model);
		
		for (ApplicationId appId : appIds) {
			YarnApplicationState state = waitState(appId, 30, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
			assertNotNull(state);
			state = waitState(appId, 60, TimeUnit.SECONDS, YarnApplicationState.FINISHED);
			assertEquals(state, YarnApplicationState.FINISHED);
		}
		
		File lrTmpFile = new File("/tmp/lr_trained.txt");
		File nnTmpFile = new File("/tmp/nn_trained.txt");
		
		assertTrue(lrTmpFile.exists());
		assertTrue(nnTmpFile.exists());
	}
}
