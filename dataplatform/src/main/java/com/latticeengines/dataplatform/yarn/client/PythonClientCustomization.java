package com.latticeengines.dataplatform.yarn.client;

import org.apache.hadoop.conf.Configuration;

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

}
