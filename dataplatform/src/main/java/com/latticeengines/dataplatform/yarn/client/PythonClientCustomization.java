package com.latticeengines.dataplatform.yarn.client;

import java.io.File;
import java.util.Collection;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.yarn.fs.LocalResourcesFactoryBean;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;

import com.latticeengines.dataplatform.exposed.domain.Classifier;
import com.latticeengines.dataplatform.runtime.execution.python.PythonContainerProperty;
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
	public String getContainerLauncherContextFile(Properties properties) {
		return "/python/dataplatform-python-appmaster-context.xml";
	}

	@Override
	public void beforeCreateLocalLauncherContextFile(Properties properties) {
		try {
			String metadata = properties.getProperty(PythonContainerProperty.METADATA.name());
			Classifier classifier = JsonHelper.deserialize(metadata, Classifier.class);
			properties.put(PythonContainerProperty.TRAINING.name(), classifier.getTrainingDataHdfsPath());
			properties.put(PythonContainerProperty.TEST.name(), classifier.getTestDataHdfsPath());
			properties.put(PythonContainerProperty.PYTHONSCRIPT.name(), classifier.getPythonScriptHdfsPath());
			properties.put(PythonContainerProperty.SCHEMA.name(), classifier.getSchemaHdfsPath());
			
			File metadataFile = new File("metadata-" + System.currentTimeMillis() +".json");
			FileUtils.writeStringToFile(metadataFile, metadata);
			properties.put(PythonContainerProperty.METADATA.name(), metadataFile.getAbsolutePath());
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public Collection<CopyEntry> getCopyEntries(Properties containerProperties) {
		Collection<CopyEntry> copyEntries = super.getCopyEntries(containerProperties);
		String metadataFilePath = containerProperties.getProperty(ContainerProperty.METADATA.name());
		copyEntries.add(new LocalResourcesFactoryBean.CopyEntry(
				"file:" + metadataFilePath, "/app/dataplatform", false));
		return copyEntries;
				
	}
}
