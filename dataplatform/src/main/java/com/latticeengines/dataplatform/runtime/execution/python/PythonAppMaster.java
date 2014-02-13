package com.latticeengines.dataplatform.runtime.execution.python;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.springframework.yarn.am.ContainerLauncherInterceptor;
import org.springframework.yarn.am.StaticEventingAppmaster;
import org.springframework.yarn.am.container.AbstractLauncher;

public class PythonAppMaster extends StaticEventingAppmaster implements ContainerLauncherInterceptor {

	private final static Log log = LogFactory.getLog(PythonAppMaster.class);
	
	@Override
	protected void onInit() throws Exception {
		super.onInit();
		if (getLauncher() instanceof AbstractLauncher) {
			((AbstractLauncher) getLauncher()).addInterceptor(this);
		}
	}
	
	@Override
	public void setParameters(Properties parameters) {
		if (parameters == null) {
			return;
		}
		for (Map.Entry<Object, Object> parameter : parameters.entrySet()) {
			log.info("Key = " + parameter.getKey().toString() + " Value = " + parameter.getValue().toString());
		}
		super.setParameters(parameters);
	}
	
	@Override
	public ContainerLaunchContext preLaunch(Container container,
			ContainerLaunchContext context) {
		return context;
	}

}
