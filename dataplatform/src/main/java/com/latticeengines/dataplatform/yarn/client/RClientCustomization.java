package com.latticeengines.dataplatform.yarn.client;

import org.apache.hadoop.conf.Configuration;

public class RClientCustomization extends DefaultYarnClientCustomization {

	public RClientCustomization(Configuration configuration) {
		super(configuration);
	}

	@Override
	public String getClientId() {
		return "RClient";
	}

	@Override
	public String getContainerLauncherContextFile() {
		return "R/dataplatform-R-appmaster-context.xml";
	}

}

