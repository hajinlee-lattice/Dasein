package com.latticeengines.dataplatform.yarn.client;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

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

	private Configuration configuration;

	public DefaultYarnClientCustomization(Configuration configuration) {
		this.configuration = configuration;
	}

	@Override
	public ResourceLocalizer getResourceLocalizer(String containerLaunchContextFile) {
		Collection<CopyEntry> copyEntries = getCopyEntries();
		copyEntries.add(new LocalResourcesFactoryBean.CopyEntry(
				"file:" + containerLaunchContextFile,
				"/app/dataplatform", false));
		return new DefaultResourceLocalizer(configuration, getHdfsEntries(), copyEntries);
	}

	@Override
	public Collection<CopyEntry> getCopyEntries() {
		Collection<LocalResourcesFactoryBean.CopyEntry> copyEntries = new ArrayList<LocalResourcesFactoryBean.CopyEntry>(); 
		return copyEntries;
	}

	@Override
	public Collection<TransferEntry> getHdfsEntries() {
		String defaultFs = configuration
				.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
		Collection<LocalResourcesFactoryBean.TransferEntry> hdfsEntries = new ArrayList<LocalResourcesFactoryBean.TransferEntry>();
		hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(
				LocalResourceType.ARCHIVE, //
				LocalResourceVisibility.PUBLIC, "/lib/*", defaultFs, defaultFs,
				false));
		hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(
				LocalResourceType.FILE, //
				LocalResourceVisibility.PUBLIC, "/app/dataplatform/*",
				defaultFs, defaultFs, false));
		return hdfsEntries;
	}

	@Override
	public String getClientId() {
		return "defaultYarnClient";
	}

	@Override
	public int getMemory() {
		return -1;
	}

	@Override
	public int getPriority() {
		return -1;
	}

	@Override
	public String getQueue() {
		return null;
	}

	@Override
	public int getVirtualcores() {
		return -1;
	}

	@Override
	public String getContainerLauncherContextFile() {
		return "default/dataplatform-default-appmaster-context.xml";
	}

	@Override
	public List<String> getCommands(String containerLauncherContextFile) {
		File contextFile = new File(containerLauncherContextFile);
		if (!contextFile.exists()) {
			throw new IllegalStateException("Container launcher context file " + containerLauncherContextFile
			+ " does not exist.");
		}
		return Arrays
				.<String> asList(new String[] {
						"$JAVA_HOME/bin/java", //
						"org.springframework.yarn.am.CommandLineAppmasterRunnerForLocalContextFile", //
						contextFile.getName(), // 
						"yarnAppmaster", //
						"1><LOG_DIR>/Appmaster.stdout", //
						"2><LOG_DIR>/Appmaster.stderr" });
	}

}
