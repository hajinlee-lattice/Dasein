package com.latticeengines.dataplatform.yarn.client;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import com.latticeengines.dataplatform.exposed.domain.Classifier;
import com.latticeengines.dataplatform.runtime.execution.python.PythonContainerProperty;
import com.latticeengines.dataplatform.util.HdfsHelper;
import com.latticeengines.dataplatform.util.JsonHelper;

public class PythonClientCustomization extends DefaultYarnClientCustomization {

	public PythonClientCustomization(Configuration configuration) {
		super(configuration);
	}

	@Override
	public String getClientId() {
		return "pythonClient";
	}

	@Override
	public String getContainerLauncherContextFile() {
		return "python/dataplatform-python-appmaster-context.xml";
	}

	@Override
	public void beforeCreateLocalLauncherContextFile(Properties properties) {
		try {
			String schema = properties.getProperty(PythonContainerProperty.SCHEMA.name());
			String jsonContents = HdfsHelper.getHdfsFileContents(configuration, schema);
			Classifier classifier = JsonHelper.deserialize(jsonContents, Classifier.class);
			properties.put(PythonContainerProperty.TRAINING.name(), classifier.getTrainingDataHdfsPath());
			properties.put(PythonContainerProperty.TEST.name(), classifier.getTestDataHdfsPath());
			properties.put(PythonContainerProperty.PYTHONSCRIPT.name(), classifier.getPythonScriptHdfsPath());
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

}
