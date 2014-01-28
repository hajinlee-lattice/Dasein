package com.latticeengines.dataplatform.yarn.client;

import java.util.Collection;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.springframework.yarn.fs.LocalResourcesFactoryBean;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;

import com.latticeengines.dataplatform.runtime.execution.python.PythonContainerProperty;

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
	public Collection<CopyEntry> getCopyEntries(Properties containerProperties) {
		Collection<LocalResourcesFactoryBean.CopyEntry> copyEntries = super.getCopyEntries(containerProperties);
		String pythonScript = containerProperties.getProperty(PythonContainerProperty.PYTHONSCRIPT.name());
		copyEntries.add(new LocalResourcesFactoryBean.CopyEntry("file:" + pythonScript,
				"/app/dataplatform", false));
		return copyEntries;
	}

}
