package com.latticeengines.dataplatform.runtime.execution.python;

import java.io.File;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.yarn.container.YarnContainer;

public class PythonContainer implements YarnContainer {
	
	private static final Log log = LogFactory.getLog(PythonContainer.class);
	private String trainingFileName = null;
	private String testFileName = null;
	private String pythonScript = null;

	@Override
	public void run() {
		File trainingFile = new File(trainingFileName);
		log.info("Training file " + trainingFile.getAbsolutePath() + " exists = " + trainingFile.exists());
		File testFile = new File(testFileName);
		log.info("Test file " + testFile.getAbsolutePath() + " exists = " + testFile.exists());
		File pythonFile = new File(pythonScript);
		log.info("Python script " + pythonFile.getAbsolutePath() + " exists = " + pythonFile.exists());
	}

	@Override
	public void setEnvironment(Map<String, String> environment) {
	}

	@Override
	public void setParameters(Properties parameters) {
		for (Entry<Object, Object> parameter : parameters.entrySet()) {
			log.info("Key = " + parameter.getKey().toString() + " Value = " + parameter.getValue().toString());
		}
		
		trainingFileName = parameters.getProperty(PythonContainerProperty.TRAINING.name());
		testFileName = parameters.getProperty(PythonContainerProperty.TEST.name());
		pythonScript = parameters.getProperty(PythonContainerProperty.PYTHONSCRIPT.name());
		trainingFileName = getFileNameOnly(trainingFileName);
		testFileName = getFileNameOnly(testFileName);
		pythonScript = getFileNameOnly(pythonScript);
	}
	
	private String getFileNameOnly(String path) {
		return path.substring(path.lastIndexOf("/")+1, path.length());
	}

}
