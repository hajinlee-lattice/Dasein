package com.latticeengines.dataplatform.runtime.execution.python;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
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
		if (context == null) {
			log.info("Container launch context is null.");
			return context;
		}
		Configuration conf = new YarnConfiguration();
		String fs = conf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
		Properties props = getParameters();
		Map<String, LocalResource> resources = context.getLocalResources();
		
		String trainingDataPath = fs + props.getProperty(PythonContainerProperty.TRAINING_HDFS_PATH.name());
		String trainingFileName = props.getProperty(PythonContainerProperty.TRAINING_FILE_NAME.name());
		String testDataPath = fs + props.getProperty(PythonContainerProperty.TEST_HDFS_PATH.name());
		String testFileName = props.getProperty(PythonContainerProperty.TEST_FILE_NAME.name());
		
		try {
			LocalResource trainingDataRes = createLocalResource(new Path(trainingDataPath), conf);
			LocalResource testDataRes = createLocalResource(new Path(testDataPath), conf);
			resources.put(trainingFileName, trainingDataRes);
			resources.put(testFileName, testDataRes);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		
		return context;
	}
	
    private LocalResource createLocalResource(Path dataPath, Configuration conf) throws IOException {
    	LocalResource localResource = Records.newRecord(LocalResource.class);
        FileStatus fileStat = FileSystem.get(conf).getFileStatus(dataPath);
        localResource.setResource(ConverterUtils.getYarnUrlFromPath(dataPath));
        localResource.setSize(fileStat.getLen());
        localResource.setTimestamp(fileStat.getModificationTime());
        localResource.setType(LocalResourceType.FILE);
        localResource.setVisibility(LocalResourceVisibility.PUBLIC);
        return localResource;
    }

}
