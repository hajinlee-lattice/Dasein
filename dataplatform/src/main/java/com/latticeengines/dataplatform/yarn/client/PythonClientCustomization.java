package com.latticeengines.dataplatform.yarn.client;

import java.util.Collection;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.springframework.yarn.fs.LocalResourcesFactoryBean;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.TransferEntry;

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

	@Override
	public Collection<TransferEntry> getHdfsEntries(Properties containerProperties) {
		String defaultFs = configuration.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
		Collection<LocalResourcesFactoryBean.TransferEntry> hdfsEntries = super.getHdfsEntries(containerProperties);
		String trainingHdfsFilePath = containerProperties.getProperty(PythonContainerProperty.TRAINING_HDFS_PATH.name());
		String testHdfsFilePath = containerProperties.getProperty(PythonContainerProperty.TEST_HDFS_PATH.name());
		hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(
				LocalResourceType.FILE, //
				LocalResourceVisibility.PUBLIC, trainingHdfsFilePath,
				defaultFs, defaultFs, false));
		hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(
				LocalResourceType.FILE, //
				LocalResourceVisibility.PUBLIC, testHdfsFilePath,
				defaultFs, defaultFs, false));
		return hdfsEntries;
	}

}
