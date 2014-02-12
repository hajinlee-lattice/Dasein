package com.latticeengines.dataplatform.yarn.client;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.springframework.yarn.fs.DefaultResourceLocalizer;
import org.springframework.yarn.fs.LocalResourcesFactoryBean;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.TransferEntry;
import org.springframework.yarn.fs.ResourceLocalizer;

public class DefaultYarnClientCustomization implements YarnClientCustomization {
	private static final Log log = LogFactory.getLog(DefaultYarnClientCustomization.class);

	protected Configuration configuration;

	public DefaultYarnClientCustomization(Configuration configuration) {
		this.configuration = configuration;
	}

	@Override
	public ResourceLocalizer getResourceLocalizer(Properties containerProperties) {
		return new DefaultResourceLocalizer(configuration, getHdfsEntries(containerProperties), getCopyEntries(containerProperties));
	}
	
	protected String getJobDir(Properties containerProperties) {
		return "/app/dataplatform/" + containerProperties.getProperty(ContainerProperty.JOBDIR.name());
	}

	@Override
	public Collection<CopyEntry> getCopyEntries(Properties containerProperties) {
		Collection<LocalResourcesFactoryBean.CopyEntry> copyEntries = new ArrayList<LocalResourcesFactoryBean.CopyEntry>(); 
		String containerLaunchContextFile = containerProperties.getProperty(ContainerProperty.APPMASTER_CONTEXT_FILE.name());
		copyEntries.add(new LocalResourcesFactoryBean.CopyEntry(
				"file:" + containerLaunchContextFile,
				getJobDir(containerProperties), false));
		
		return copyEntries;
	}

	@Override
	public Collection<TransferEntry> getHdfsEntries(Properties containerProperties) {
		String defaultFs = configuration.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
		Collection<LocalResourcesFactoryBean.TransferEntry> hdfsEntries = new ArrayList<LocalResourcesFactoryBean.TransferEntry>();
		hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(
				LocalResourceType.FILE, //
				LocalResourceVisibility.PUBLIC, //
				"/lib/*", //
				defaultFs, //
				defaultFs, //
				false));
		hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(
				LocalResourceType.FILE, //
				LocalResourceVisibility.PUBLIC, //
				"/app/dataplatform/*.properties", //
				defaultFs, //
				defaultFs, //
				false));
		hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(
				LocalResourceType.FILE, //
				LocalResourceVisibility.PUBLIC, //
				"/app/dataplatform/*.jar", //
				defaultFs, //
				defaultFs, //
				false));
		hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(
				LocalResourceType.FILE, //
				LocalResourceVisibility.PUBLIC, //
				getJobDir(containerProperties) + "/*", //
				defaultFs, //
				defaultFs, //
				false));
		return hdfsEntries;
	}

	@Override
	public String getClientId() {
		return "defaultYarnClient";
	}

	@Override
	public int getMemory(Properties properties) {
		String memory = properties.getProperty(AppMasterProperty.MEMORY.name(), "-1");
		return Integer.parseInt(memory);
	}

	@Override
	public int getPriority(Properties properties) {
		String priority = properties.getProperty(AppMasterProperty.PRIORITY.name(), "-1");
		return Integer.parseInt(priority);
	}

	@Override
	public String getQueue(Properties properties) {
		return properties.getProperty(AppMasterProperty.QUEUE.name());
	}

	@Override
	public int getVirtualcores(Properties properties) {
		String virtualCores = properties.getProperty(AppMasterProperty.VIRTUALCORES.name(), "-1");
		return Integer.parseInt(virtualCores);
	}

	@Override
	public String getContainerLauncherContextFile(Properties properties) {
		return "/default/dataplatform-default-appmaster-context.xml";
	}

	@Override
	public List<String> getCommands(Properties containerProperties) {
		String containerLaunchContextFile = containerProperties.getProperty(ContainerProperty.APPMASTER_CONTEXT_FILE.name());
		if (containerLaunchContextFile == null) {
			throw new IllegalStateException("Property " + ContainerProperty.APPMASTER_CONTEXT_FILE + " does not exist.");
		}
		File contextFile = new File(containerLaunchContextFile);
		if (!contextFile.exists()) {
			throw new IllegalStateException("Container launcher context file " + containerLaunchContextFile
			+ " does not exist.");
		}
		String propStr = containerProperties.toString();
		
		return Arrays.<String> asList(new String[] {
						"$JAVA_HOME/bin/java", //
						"org.springframework.yarn.am.CommandLineAppmasterRunnerForLocalContextFile", //
						contextFile.getName(), // 
						"yarnAppmaster", //
						propStr.substring(1, propStr.length()-1).replaceAll(",", " "), //
						"1><LOG_DIR>/Appmaster.stdout", //
						"2><LOG_DIR>/Appmaster.stderr" });
	}

	@Override
	public void beforeCreateLocalLauncherContextFile(Properties properties) {
	}

	@Override
	public void validate(Properties appMasterProperties,
			Properties containerProperties) {
		
	}

	@Override
	public void finalize(Properties appMasterProperties, Properties containerProperties) {
		String contextFileName = containerProperties.getProperty(ContainerProperty.APPMASTER_CONTEXT_FILE.name());
		
		if (contextFileName != null) {
			File contextFile = new File(contextFileName);
			if (!contextFile.delete()) {
				log.warn("Could not delete launch context file " + contextFileName);
			}
		}
	}

}
