package com.latticeengines.dataplatform.yarn.client;

import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.TransferEntry;
import org.springframework.yarn.fs.ResourceLocalizer;

public interface YarnClientCustomization {
	
	String getClientId();

	ResourceLocalizer getResourceLocalizer(Properties properties);
	
	Collection<CopyEntry> getCopyEntries(Properties properties);
	
	Collection<TransferEntry> getHdfsEntries(Properties properties);
	
	int getMemory();
	
	int getPriority();
	
	String getQueue();
	
	int getVirtualcores();
	
	String getContainerLauncherContextFile();
	
	List<String> getCommands(Properties properties);
}
